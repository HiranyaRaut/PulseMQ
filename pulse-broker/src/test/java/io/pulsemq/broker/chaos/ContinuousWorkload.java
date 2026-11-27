package io.pulsemq.broker.chaos;

import io.pulsemq.broker.PulseServer;
import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.client.RecordMetadata;
import io.pulsemq.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resilient continuous workload generator and verifier.
 * Generates records continuously over Virtual Threads across active chaos events,
 * reconnecting to newly elected leaders upon broker crashes and verifying
 * 100% data integrity, zero message loss, and strict monotonic offset ordering.
 */
public class ContinuousWorkload implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ContinuousWorkload.class);

    public record WorkloadRecord(String key, String value, long offset, int leaderBrokerId, long timestamp) {}

    public record IntegrityReport(
            int acknowledgedCount,
            int verifiedCount,
            long lastOffset,
            int errorCount,
            boolean isMonotonic,
            boolean hasZeroGaps
    ) {}

    private final ChaosCluster cluster;
    private final String topic;
    private final int partition;
    private final short acks;
    private final long sendIntervalMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<WorkloadRecord> acknowledged = new CopyOnWriteArrayList<>();
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private Thread workerThread;

    public ContinuousWorkload(ChaosCluster cluster, String topic, int partition, short acks, long sendIntervalMs) {
        this.cluster = cluster;
        this.topic = topic;
        this.partition = partition;
        this.acks = acks;
        this.sendIntervalMs = sendIntervalMs;
    }

    public ContinuousWorkload(ChaosCluster cluster, String topic, int partition, short acks) {
        this(cluster, topic, partition, acks, 15L);
    }

    /**
     * Starts continuous load generation in a Virtual Thread.
     */
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        log.info("Starting ContinuousWorkload on topic '{}', partition {}, acks={}", topic, partition, acks);
        workerThread = Thread.ofVirtual().name("chaos-workload-" + topic).start(this::runProducerLoop);
    }

    private void runProducerLoop() {
        TopicPartition tp = new TopicPartition(topic, partition);
        PulseProducer producer = null;
        int connectedLeaderId = -1;

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            PulseServer leader = null;
            try {
                leader = cluster.waitForLeader(tp, Duration.ofSeconds(3));
            } catch (InterruptedException e) {
                break;
            }

            if (leader == null || !leader.isRunning()) {
                if (producer != null) {
                    try { producer.close(); } catch (Exception ignored) {}
                    producer = null;
                    connectedLeaderId = -1;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    break;
                }
                continue;
            }

            int currentLeaderId = leader.config().brokerId();
            if (producer == null || connectedLeaderId != currentLeaderId) {
                if (producer != null) {
                    try { producer.close(); } catch (Exception ignored) {}
                }
                try {
                    producer = new PulseProducer("127.0.0.1", leader.boundPort(), "chaos-producer", acks);
                    connectedLeaderId = currentLeaderId;
                    log.info("ContinuousWorkload connected to leader broker [{}] at port {}",
                            connectedLeaderId, leader.boundPort());
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    producer = null;
                    connectedLeaderId = -1;
                    try { Thread.sleep(50); } catch (InterruptedException ie) { break; }
                    continue;
                }
            }

            int seq = sequenceNumber.get();
            String key = "key-" + seq;
            String val = "payload-" + seq + "-ts-" + System.currentTimeMillis();

            try {
                RecordMetadata meta = producer.send(new ProducerRecord(topic, key, val))
                        .get(3, TimeUnit.SECONDS);

                acknowledged.add(new WorkloadRecord(key, val, meta.offset(), connectedLeaderId, meta.timestamp()));
                sequenceNumber.incrementAndGet();

                if (sendIntervalMs > 0) {
                    Thread.sleep(sendIntervalMs);
                }
            } catch (Exception e) {
                errorCount.incrementAndGet();
                log.debug("Transient produce error on broker [{}]: {}", connectedLeaderId, e.getMessage());
                if (producer != null) {
                    try { producer.close(); } catch (Exception ignored) {}
                    producer = null;
                    connectedLeaderId = -1;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }

        if (producer != null) {
            try { producer.close(); } catch (Exception ignored) {}
        }
        log.info("ContinuousWorkload producer loop terminated. Acknowledged: {}, Errors caught: {}",
                acknowledged.size(), errorCount.get());
    }

    /**
     * Stops load generation and awaits thread termination.
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (workerThread != null) {
            workerThread.interrupt();
            try {
                workerThread.join(3000);
            } catch (InterruptedException ignored) {}
        }
    }

    /**
     * Polls all messages from the cluster and verifies 100% data integrity against acknowledged records.
     */
    public IntegrityReport verifyDataIntegrity(Duration timeout) throws Exception {
        stop();

        TopicPartition tp = new TopicPartition(topic, partition);
        PulseServer leader = cluster.waitForLeader(tp, timeout);
        if (leader == null) {
            throw new IllegalStateException("Cannot verify data integrity: no leader available for " + tp);
        }

        int expectedCount = acknowledged.size();
        log.info("Verifying data integrity on leader [{}] at port {}. Expected records: {}",
                leader.config().brokerId(), leader.boundPort(), expectedCount);

        List<ConsumerRecord> received = new ArrayList<>();
        try (PulseConsumer consumer = new PulseConsumer("127.0.0.1", leader.boundPort(), "chaos-verifier-" + UUID.randomUUID())) {
            consumer.assign(tp);

            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (received.size() < expectedCount && System.currentTimeMillis() < deadline) {
                List<ConsumerRecord> batch = consumer.poll(Duration.ofMillis(300));
                received.addAll(batch);
            }
        }

        Map<Long, ConsumerRecord> recordsByOffset = new HashMap<>();
        boolean isMonotonic = true;
        boolean hasZeroGaps = true;
        long lastOffset = -1L;

        for (int i = 0; i < received.size(); i++) {
            ConsumerRecord r = received.get(i);
            long off = r.offset();
            recordsByOffset.put(off, r);

            if (off <= lastOffset && lastOffset != -1L) {
                isMonotonic = false;
            }
            if (i > 0 && off != lastOffset + 1) {
                hasZeroGaps = false;
            }
            lastOffset = off;
        }

        // Verify that every acknowledged record was successfully committed and matches expected data
        for (WorkloadRecord ack : acknowledged) {
            ConsumerRecord cr = recordsByOffset.get(ack.offset());
            if (cr == null) {
                throw new AssertionError("Missing acknowledged record at offset " + ack.offset() + ": " + ack);
            }
            if (!ack.key().equals(cr.keyAsString()) || !ack.value().equals(cr.valueAsString())) {
                throw new AssertionError("Data corruption at offset " + ack.offset() +
                        ": expected " + ack + ", but found " + cr);
            }
        }

        IntegrityReport report = new IntegrityReport(
                expectedCount,
                received.size(),
                lastOffset,
                errorCount.get(),
                isMonotonic,
                hasZeroGaps
        );
        log.info("Integrity check report: {}", report);
        return report;
    }

    public List<WorkloadRecord> acknowledgedRecords() {
        return Collections.unmodifiableList(acknowledged);
    }

    public int acknowledgedCount() {
        return acknowledged.size();
    }

    public int errorCount() {
        return errorCount.get();
    }

    @Override
    public void close() {
        stop();
    }
}
