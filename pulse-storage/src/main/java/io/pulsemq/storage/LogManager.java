package io.pulsemq.storage;

import io.pulsemq.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Top-level storage manager responsible for loading, creating, and maintaining
 * partition commit logs across broker storage directories.
 */
public class LogManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    private final File dataDir;
    private final LogConfig defaultConfig;
    private final Map<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private volatile boolean closed = false;

    public LogManager(File dataDir, LogConfig defaultConfig) throws Exception {
        this.dataDir = dataDir;
        this.defaultConfig = defaultConfig;

        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new IOException("Failed to create log manager data directory: " + dataDir.getAbsolutePath());
        }

        // Initialize background tasks with Virtual Threads / Scheduled Executor
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pulse-log-flusher-and-retention");
            t.setDaemon(true);
            return t;
        });

        loadAllLogs();
        startBackgroundTasks();
    }

    private void loadAllLogs() throws Exception {
        File[] subdirs = dataDir.listFiles(File::isDirectory);
        if (subdirs == null) return;

        for (File dir : subdirs) {
            String dirName = dir.getName();
            int dashIdx = dirName.lastIndexOf('-');
            if (dashIdx > 0 && dashIdx < dirName.length() - 1) {
                try {
                    String topic = dirName.substring(0, dashIdx);
                    int partition = Integer.parseInt(dirName.substring(dashIdx + 1));
                    TopicPartition tp = new TopicPartition(topic, partition);
                    PartitionLog partitionLog = new PartitionLog(tp, dir, defaultConfig);
                    logs.put(tp, partitionLog);
                    log.info("Loaded partition log: {} with {} segments, LEO: {}",
                            tp, partitionLog.segments().size(), partitionLog.logEndOffset());
                } catch (NumberFormatException ignored) {
                    // Not a partition directory
                }
            }
        }
    }

    private void startBackgroundTasks() {
        // Periodic flush task
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                flushAllLogs();
            } catch (Throwable t) {
                log.error("Error during scheduled log flush", t);
            }
        }, defaultConfig.flushIntervalMs(), defaultConfig.flushIntervalMs(), TimeUnit.MILLISECONDS);

        // Retention cleaner task
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                cleanupRetention();
            } catch (Throwable t) {
                log.error("Error during scheduled log retention cleanup", t);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public PartitionLog getOrCreateLog(TopicPartition tp) throws Exception {
        return getOrCreateLog(tp, defaultConfig);
    }

    public PartitionLog getOrCreateLog(TopicPartition tp, LogConfig config) throws Exception {
        PartitionLog existing = logs.get(tp);
        if (existing != null) {
            return existing;
        }

        synchronized (this) {
            existing = logs.get(tp);
            if (existing != null) {
                return existing;
            }

            File dir = new File(dataDir, tp.topic() + "-" + tp.partition());
            PartitionLog created = new PartitionLog(tp, dir, config);
            logs.put(tp, created);
            return created;
        }
    }

    public PartitionLog getLog(TopicPartition tp) {
        return logs.get(tp);
    }

    public void flushAllLogs() {
        for (PartitionLog pl : logs.values()) {
            try {
                pl.flush();
            } catch (Exception e) {
                log.error("Failed to flush log for {}", pl.topicPartition(), e);
            }
        }
    }

    public void cleanupRetention() {
        for (PartitionLog pl : logs.values()) {
            try {
                int cleaned = pl.cleanupRetention();
                if (cleaned > 0) {
                    log.info("Retention cleanup deleted {} segments from {}", cleaned, pl.topicPartition());
                }
            } catch (Exception e) {
                log.error("Failed retention cleanup for {}", pl.topicPartition(), e);
            }
        }
    }

    public Map<TopicPartition, PartitionLog> allLogs() {
        return logs;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}

        for (PartitionLog pl : logs.values()) {
            try {
                pl.close();
            } catch (Exception e) {
                log.error("Error closing log for {}", pl.topicPartition(), e);
            }
        }
    }
}
