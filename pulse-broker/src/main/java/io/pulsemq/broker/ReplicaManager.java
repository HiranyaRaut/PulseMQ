package io.pulsemq.broker;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.FetchRequest;
import io.pulsemq.protocol.FetchResponse;
import io.pulsemq.protocol.ProduceResponse;
import io.pulsemq.storage.FileRecords;
import io.pulsemq.storage.LogConfig;
import io.pulsemq.storage.LogManager;
import io.pulsemq.storage.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages partition replication, In-Sync Replicas (ISR), High Watermark advancement,
 * and producer acks semantics.
 */
public class ReplicaManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);

    private final BrokerConfig config;
    private final LogManager logManager;
    private final Map<TopicPartition, PartitionState> partitions = new ConcurrentHashMap<>();

    public static class PartitionState {
        private final TopicPartition tp;
        private volatile int leaderId;
        private volatile int leaderEpoch;
        private final List<Integer> replicas = new ArrayList<>();
        private final Set<Integer> isr = ConcurrentHashMap.newKeySet();

        public PartitionState(TopicPartition tp, int leaderId, int leaderEpoch, List<Integer> replicas) {
            this.tp = tp;
            this.leaderId = leaderId;
            this.leaderEpoch = leaderEpoch;
            this.replicas.addAll(replicas);
            this.isr.addAll(replicas);
        }

        public TopicPartition topicPartition() { return tp; }
        public int leaderId() { return leaderId; }
        public int leaderEpoch() { return leaderEpoch; }
        public List<Integer> replicas() { return Collections.unmodifiableList(replicas); }
        public List<Integer> isr() { return new ArrayList<>(isr); }
        public boolean isLeader(int brokerId) { return leaderId == brokerId; }
    }

    public ReplicaManager(BrokerConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;
    }

    public PartitionState getOrCreatePartition(TopicPartition tp) {
        return partitions.computeIfAbsent(tp, k ->
                new PartitionState(tp, config.brokerId(), 0, List.of(config.brokerId()))
        );
    }

    public Map<TopicPartition, ProduceResponse.PartitionProduceResult> appendRecords(
            short acks,
            Map<TopicPartition, RecordBatch> batches
    ) {
        Map<TopicPartition, ProduceResponse.PartitionProduceResult> results = new HashMap<>(batches.size());

        for (Map.Entry<TopicPartition, RecordBatch> entry : batches.entrySet()) {
            TopicPartition tp = entry.getKey();
            RecordBatch batch = entry.getValue();

            PartitionState state = getOrCreatePartition(tp);
            if (!state.isLeader(config.brokerId())) {
                results.put(tp, new ProduceResponse.PartitionProduceResult(
                        ErrorCode.NOT_LEADER_OR_FOLLOWER, -1L, -1L));
                continue;
            }

            if (acks == -1 && state.isr().size() < config.minInsyncReplicas()) {
                results.put(tp, new ProduceResponse.PartitionProduceResult(
                        ErrorCode.NOT_ENOUGH_REPLICAS, -1L, -1L));
                continue;
            }

            try {
                PartitionLog partitionLog = logManager.getOrCreateLog(tp);
                PartitionLog.LogAppendResult appendResult = partitionLog.append(batch);

                // If ISR consists only of the leader, advance high watermark immediately
                if (state.isr().size() <= 1) {
                    partitionLog.updateHighWatermark(partitionLog.logEndOffset());
                }

                results.put(tp, new ProduceResponse.PartitionProduceResult(
                        ErrorCode.NONE,
                        appendResult.baseOffset(),
                        appendResult.maxTimestamp()
                ));
            } catch (Exception e) {
                log.error("Failed to append records to {}", tp, e);
                results.put(tp, new ProduceResponse.PartitionProduceResult(
                        ErrorCode.UNKNOWN_SERVER_ERROR, -1L, -1L));
            }
        }

        return results;
    }

    public Map<TopicPartition, FetchResponse.PartitionFetchResult> readRecords(
            int maxWaitMs,
            int minBytes,
            int maxBytes,
            Map<TopicPartition, FetchRequest.PartitionFetchData> partitionFetchData
    ) {
        Map<TopicPartition, FetchResponse.PartitionFetchResult> results = new HashMap<>(partitionFetchData.size());

        for (Map.Entry<TopicPartition, FetchRequest.PartitionFetchData> entry : partitionFetchData.entrySet()) {
            TopicPartition tp = entry.getKey();
            FetchRequest.PartitionFetchData pfd = entry.getValue();

            PartitionState state = partitions.get(tp);
            if (state == null || !state.isLeader(config.brokerId())) {
                // If partition log exists on broker, allow read or return NOT_LEADER
                PartitionLog pLog = logManager.getLog(tp);
                if (pLog == null) {
                    results.put(tp, new FetchResponse.PartitionFetchResult(
                            ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, 0L, null));
                    continue;
                }
            }

            try {
                PartitionLog partitionLog = logManager.getOrCreateLog(tp);
                long fetchOffset = pfd.fetchOffset();

                if (fetchOffset > partitionLog.logEndOffset() || fetchOffset < partitionLog.logStartOffset()) {
                    results.put(tp, new FetchResponse.PartitionFetchResult(
                            ErrorCode.OFFSET_OUT_OF_BOUNDS, partitionLog.highWatermark(), null));
                    continue;
                }

                FileRecords fileRecords = partitionLog.read(
                        fetchOffset,
                        pfd.maxBytes(),
                        PartitionLog.IsolationLevel.READ_UNCOMMITTED
                );

                byte[] bytes = null;
                if (fileRecords.sizeInBytes() > 0) {
                    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(fileRecords.sizeInBytes());
                    fileRecords.channel().read(buf, fileRecords.position());
                    buf.flip();
                    bytes = buf.array();
                }

                results.put(tp, new FetchResponse.PartitionFetchResult(
                        ErrorCode.NONE,
                        partitionLog.highWatermark(),
                        bytes
                ));
            } catch (Exception e) {
                log.error("Failed to read records from {}", tp, e);
                results.put(tp, new FetchResponse.PartitionFetchResult(
                        ErrorCode.UNKNOWN_SERVER_ERROR, 0L, null));
            }
        }

        return results;
    }

    public Map<TopicPartition, PartitionState> allPartitions() {
        return partitions;
    }

    public LogManager logManager() {
        return logManager;
    }

    @Override
    public void close() {
        // ReplicaManager cleanup
    }
}
