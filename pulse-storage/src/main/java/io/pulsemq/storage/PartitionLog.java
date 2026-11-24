package io.pulsemq.storage;

import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the append-only commit log for a single topic partition.
 * Consists of an ordered sequence of immutable LogSegments and one active appendable LogSegment.
 */
public class PartitionLog implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PartitionLog.class);

    private final TopicPartition topicPartition;
    private final File dir;
    private final LogConfig config;
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private volatile LogSegment activeSegment;
    private volatile long logStartOffset = 0L;
    private volatile long highWatermark = 0L;
    private volatile boolean closed = false;

    public record LogAppendResult(
            long baseOffset,
            long lastOffset,
            long maxTimestamp,
            int bytesWritten
    ) {}

    public enum IsolationLevel {
        READ_UNCOMMITTED,
        READ_COMMITTED
    }

    public PartitionLog(TopicPartition topicPartition, File dir, LogConfig config) throws Exception {
        this.topicPartition = topicPartition;
        this.dir = dir;
        this.config = config;

        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed to create partition log directory: " + dir.getAbsolutePath());
        }

        loadSegments();
    }

    private void loadSegments() throws Exception {
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles != null && logFiles.length > 0) {
            for (File file : logFiles) {
                String name = file.getName();
                long baseOffset = Long.parseLong(name.substring(0, name.indexOf('.')));
                LogSegment segment = new LogSegment(dir, baseOffset, config);
                segments.put(baseOffset, segment);
            }
        }

        if (segments.isEmpty()) {
            roll(0L);
        } else {
            activeSegment = segments.lastEntry().getValue();
            logStartOffset = segments.firstKey();
            highWatermark = logEndOffset();
        }
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public File dir() {
        return dir;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long logEndOffset() {
        return activeSegment.lastOffset() + 1;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public void updateHighWatermark(long newHw) {
        rwLock.writeLock().lock();
        try {
            long leo = logEndOffset();
            this.highWatermark = Math.min(Math.max(highWatermark, newHw), leo);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Appends a batch of records to the active segment, rolling a new segment if necessary.
     */
    public LogAppendResult append(RecordBatch batch) throws Exception {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            long nextOffset = logEndOffset();
            RecordBatch batchToAppend = batch;
            if (batch.baseOffset() != nextOffset) {
                batchToAppend = batch.withBaseOffset(nextOffset);
            }

            if (activeSegment.size() + batchToAppend.sizeInBytes() > config.maxSegmentBytes()) {
                roll(batchToAppend.baseOffset());
            }

            activeSegment.append(batchToAppend);

            return new LogAppendResult(
                    batchToAppend.baseOffset(),
                    batchToAppend.lastOffset(),
                    batchToAppend.maxTimestamp(),
                    batchToAppend.sizeInBytes()
            );
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Reads a slice of records starting at startOffset up to maxBytes.
     */
    public FileRecords read(long startOffset, int maxBytes, IsolationLevel isolationLevel) throws IOException {
        rwLock.readLock().lock();
        try {
            ensureOpen();
            long maxReadableOffset = (isolationLevel == IsolationLevel.READ_COMMITTED) ? highWatermark : logEndOffset();

            if (startOffset < logStartOffset || startOffset > maxReadableOffset) {
                return new FileRecords(activeSegment.logFile() != null ? activeSegment.read(startOffset, 0).channel() : null, 0, 0);
            }

            if (startOffset == maxReadableOffset) {
                return new FileRecords(null, 0, 0);
            }

            Map.Entry<Long, LogSegment> entry = segments.floorEntry(startOffset);
            if (entry == null) {
                return new FileRecords(null, 0, 0);
            }

            LogSegment segment = entry.getValue();
            return segment.read(startOffset, maxBytes);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Rolls a new active segment with the specified baseOffset.
     */
    public LogSegment roll(long newBaseOffset) throws Exception {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            if (activeSegment != null) {
                activeSegment.flush();
                activeSegment.offsetIndex().trimToValidSize();
                activeSegment.timeIndex().trimToValidSize();
            }

            LogSegment newSegment = new LogSegment(dir, newBaseOffset, config);
            segments.put(newBaseOffset, newSegment);
            activeSegment = newSegment;
            return newSegment;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Truncates the log to targetOffset (e.g. during replica reconciliation).
     */
    public void truncateTo(long targetOffset) throws IOException {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            if (targetOffset >= logEndOffset()) {
                return;
            }

            List<Long> toRemove = new ArrayList<>();
            for (Map.Entry<Long, LogSegment> entry : segments.entrySet()) {
                if (entry.getKey() > targetOffset) {
                    toRemove.add(entry.getKey());
                }
            }

            for (Long baseOff : toRemove) {
                LogSegment seg = segments.remove(baseOff);
                if (seg != null) {
                    seg.delete();
                }
            }

            if (segments.isEmpty()) {
                try {
                    roll(targetOffset);
                } catch (Exception e) {
                    throw new IOException("Failed to roll new segment during truncate", e);
                }
            } else {
                activeSegment = segments.lastEntry().getValue();
                activeSegment.truncateTo(targetOffset);
            }

            this.highWatermark = Math.min(highWatermark, logEndOffset());
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Executes retention cleanup based on time and total partition size.
     */
    public int cleanupRetention() {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            long now = System.currentTimeMillis();
            List<LogSegment> candidates = new ArrayList<>();

            for (LogSegment seg : segments.values()) {
                if (seg == activeSegment) continue; // Never delete active segment
                if (config.retentionMs() > 0 && now - seg.maxTimestamp() > config.retentionMs()) {
                    candidates.add(seg);
                }
            }

            int deletedCount = 0;
            for (LogSegment seg : candidates) {
                segments.remove(seg.baseOffset());
                seg.delete();
                deletedCount++;
            }

            if (!segments.isEmpty()) {
                logStartOffset = segments.firstKey();
            }

            return deletedCount;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void flush() throws IOException {
        rwLock.writeLock().lock();
        try {
            if (closed) return;
            for (LogSegment seg : segments.values()) {
                seg.flush();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<LogSegment> segments() {
        return Collections.unmodifiableList(new ArrayList<>(segments.values()));
    }

    public LogSegment activeSegment() {
        return activeSegment;
    }

    public LogConfig config() {
        return config;
    }

    /**
     * Atomically replaces old segments with newly cleaned segments.
     */
    public void replaceSegments(List<Long> oldBaseOffsets, List<LogSegment> newSegments) {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            for (Long baseOff : oldBaseOffsets) {
                LogSegment oldSeg = segments.remove(baseOff);
                if (oldSeg != null) {
                    oldSeg.delete();
                }
            }

            for (LogSegment newSeg : newSegments) {
                segments.put(newSeg.baseOffset(), newSeg);
            }

            if (!segments.isEmpty()) {
                logStartOffset = segments.firstKey();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        rwLock.writeLock().lock();
        try {
            if (closed) return;
            closed = true;
            for (LogSegment seg : segments.values()) {
                seg.close();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PartitionLog " + topicPartition + " is closed");
        }
    }
}
