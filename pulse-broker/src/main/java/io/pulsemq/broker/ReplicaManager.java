package io.pulsemq.broker;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.FetchRequest;
import io.pulsemq.protocol.FetchResponse;
import io.pulsemq.protocol.ProduceResponse;
import io.pulsemq.protocol.RequestHeader;
import io.pulsemq.storage.FileRecords;
import io.pulsemq.storage.LogConfig;
import io.pulsemq.storage.LogManager;
import io.pulsemq.storage.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages partition replication, In-Sync Replicas (ISR), High Watermark advancement,
 * follower fetch tracking, and producer acks semantics.
 */
public class ReplicaManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);

    private final BrokerConfig config;
    private final LogManager logManager;
    private final Map<TopicPartition, PartitionState> partitions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService isrExpirationScheduler;

    public static class PartitionState {
        private final TopicPartition tp;
        private volatile int leaderId;
        private volatile int leaderEpoch;
        private final List<Integer> replicas = new CopyOnWriteArrayList<>();
        private final Set<Integer> isr = ConcurrentHashMap.newKeySet();
        private final Map<Integer, Long> replicaLeo = new ConcurrentHashMap<>();
        private final Map<Integer, Long> replicaLastFetchTime = new ConcurrentHashMap<>();
        private final List<DelayedProduce> delayedProduces = new CopyOnWriteArrayList<>();

        public record DelayedProduce(long requiredOffset, CompletableFuture<Void> future) {}

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

        public synchronized void updateLeaderAndReplicas(int newLeaderId, int newEpoch, List<Integer> newReplicas) {
            this.leaderId = newLeaderId;
            this.leaderEpoch = newEpoch;
            this.replicas.clear();
            this.replicas.addAll(newReplicas);
            this.isr.retainAll(newReplicas);
            if (this.isr.isEmpty()) {
                this.isr.add(newLeaderId);
            }
        }

        public void updateFollowerFetch(int replicaId, long fetchOffset, long leaderLeo, long replicaLagTimeMaxMs) {
            replicaLeo.put(replicaId, fetchOffset);
            replicaLastFetchTime.put(replicaId, System.currentTimeMillis());

            // Expand ISR if follower caught up to leader LEO
            if (fetchOffset >= leaderLeo && !isr.contains(replicaId)) {
                isr.add(replicaId);
                log.info("ISR expanded for {}: added replica {}, current ISR: {}", tp, replicaId, isr);
            }
        }

        public void checkIsrExpiration(long replicaLagTimeMaxMs) {
            long now = System.currentTimeMillis();
            for (int replicaId : isr) {
                if (replicaId == leaderId) continue; // Leader never expires from ISR
                Long lastFetch = replicaLastFetchTime.get(replicaId);
                if (lastFetch != null && (now - lastFetch > replicaLagTimeMaxMs)) {
                    if (isr.size() > 1) {
                        isr.remove(replicaId);
                        log.warn("ISR shrunk for {}: removed lagging replica {}, current ISR: {}",
                                tp, replicaId, isr);
                    }
                }
            }
        }

        public long computeIsrHighWatermark(long leaderLeo) {
            long minLeo = leaderLeo;
            for (int replicaId : isr) {
                if (replicaId == leaderId) continue;
                Long fLeo = replicaLeo.get(replicaId);
                if (fLeo != null && fLeo < minLeo) {
                    minLeo = fLeo;
                }
            }
            return minLeo;
        }

        public void registerDelayedProduce(long requiredOffset, CompletableFuture<Void> future) {
            delayedProduces.add(new DelayedProduce(requiredOffset, future));
        }

        public void checkDelayedProduces(long currentHw) {
            List<DelayedProduce> completed = new ArrayList<>();
            for (DelayedProduce dp : delayedProduces) {
                if (currentHw > dp.requiredOffset()) {
                    dp.future().complete(null);
                    completed.add(dp);
                }
            }
            delayedProduces.removeAll(completed);
        }
    }

    public ReplicaManager(BrokerConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;
        this.isrExpirationScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pulse-isr-expiration-tracker");
            t.setDaemon(true);
            return t;
        });

        // Periodic ISR expiration check
        this.isrExpirationScheduler.scheduleAtFixedRate(
                this::checkIsrExpirations, 1000, 1000, TimeUnit.MILLISECONDS
        );
    }

    public void checkIsrExpirations() {
        for (PartitionState state : partitions.values()) {
            if (state.isLeader(config.brokerId())) {
                state.checkIsrExpiration(config.replicaLagTimeMaxMs());
            }
        }
    }

    public PartitionState getOrCreatePartition(TopicPartition tp) {
        return partitions.computeIfAbsent(tp, k ->
                new PartitionState(tp, config.brokerId(), 0, List.of(config.brokerId()))
        );
    }

    public PartitionState becomeLeaderOrFollower(TopicPartition tp, int leaderId, int leaderEpoch, List<Integer> replicas) {
        PartitionState state = partitions.computeIfAbsent(tp, k ->
                new PartitionState(tp, leaderId, leaderEpoch, replicas)
        );
        state.updateLeaderAndReplicas(leaderId, leaderEpoch, replicas);
        return state;
    }

    public CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionProduceResult>> appendRecordsAsync(
            short acks,
            Map<TopicPartition, RecordBatch> batches
    ) {
        Map<TopicPartition, ProduceResponse.PartitionProduceResult> immediateResults = new HashMap<>();
        List<CompletableFuture<Void>> waitFutures = new ArrayList<>();

        for (Map.Entry<TopicPartition, RecordBatch> entry : batches.entrySet()) {
            TopicPartition tp = entry.getKey();
            RecordBatch batch = entry.getValue();

            PartitionState state = getOrCreatePartition(tp);
            if (!state.isLeader(config.brokerId())) {
                immediateResults.put(tp, new ProduceResponse.PartitionProduceResult(
                        ErrorCode.NOT_LEADER_OR_FOLLOWER, -1L, -1L));
                continue;
            }

            if (acks == -1 && state.isr().size() < config.minInsyncReplicas()) {
                immediateResults.put(tp, new ProduceResponse.PartitionProduceResult(
                        ErrorCode.NOT_ENOUGH_REPLICAS, -1L, -1L));
                continue;
            }

            try {
                PartitionLog partitionLog = logManager.getOrCreateLog(tp);
                PartitionLog.LogAppendResult appendResult = partitionLog.append(batch);

                if (state.isr().size() <= 1) {
                    partitionLog.updateHighWatermark(partitionLog.logEndOffset());
                }

                ProduceResponse.PartitionProduceResult result = new ProduceResponse.PartitionProduceResult(
                        ErrorCode.NONE,
                        appendResult.baseOffset(),
                        appendResult.maxTimestamp()
                );
                immediateResults.put(tp, result);

                // If acks == all / -1 and multi-replica ISR, wait for high watermark advancement
                if (acks == -1 && state.isr().size() > 1 && partitionLog.highWatermark() <= appendResult.lastOffset()) {
                    CompletableFuture<Void> hwWaitFuture = new CompletableFuture<>();
                    state.registerDelayedProduce(appendResult.lastOffset(), hwWaitFuture);
                    waitFutures.add(hwWaitFuture.orTimeout(5000, TimeUnit.MILLISECONDS));
                }
            } catch (Exception e) {
                log.error("Failed to append records to {}", tp, e);
                immediateResults.put(tp, new ProduceResponse.PartitionProduceResult(
                        ErrorCode.UNKNOWN_SERVER_ERROR, -1L, -1L));
            }
        }

        if (waitFutures.isEmpty()) {
            return CompletableFuture.completedFuture(immediateResults);
        }

        return CompletableFuture.allOf(waitFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> immediateResults)
                .exceptionally(ex -> {
                    // On timeout, map to REQUEST_TIMED_OUT
                    Map<TopicPartition, ProduceResponse.PartitionProduceResult> timeoutResults = new HashMap<>();
                    for (TopicPartition tp : immediateResults.keySet()) {
                        timeoutResults.put(tp, new ProduceResponse.PartitionProduceResult(
                                ErrorCode.REQUEST_TIMED_OUT, -1L, -1L));
                    }
                    return timeoutResults;
                });
    }

    public Map<TopicPartition, ProduceResponse.PartitionProduceResult> appendRecords(
            short acks,
            Map<TopicPartition, RecordBatch> batches
    ) {
        try {
            return appendRecordsAsync(acks, batches).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Map<TopicPartition, ProduceResponse.PartitionProduceResult> err = new HashMap<>();
            for (TopicPartition tp : batches.keySet()) {
                err.put(tp, new ProduceResponse.PartitionProduceResult(ErrorCode.REQUEST_TIMED_OUT, -1L, -1L));
            }
            return err;
        }
    }

    public Map<TopicPartition, FetchResponse.PartitionFetchResult> readRecords(
            RequestHeader header,
            int maxWaitMs,
            int minBytes,
            int maxBytes,
            Map<TopicPartition, FetchRequest.PartitionFetchData> partitionFetchData
    ) {
        Map<TopicPartition, FetchResponse.PartitionFetchResult> results = new HashMap<>(partitionFetchData.size());

        Integer replicaId = null;
        if (header != null && header.clientId() != null && header.clientId().startsWith("pulse-replica-")) {
            try {
                replicaId = Integer.parseInt(header.clientId().substring("pulse-replica-".length()));
            } catch (NumberFormatException ignored) {}
        }

        for (Map.Entry<TopicPartition, FetchRequest.PartitionFetchData> entry : partitionFetchData.entrySet()) {
            TopicPartition tp = entry.getKey();
            FetchRequest.PartitionFetchData pfd = entry.getValue();

            PartitionState state = partitions.get(tp);
            if (state == null || !state.isLeader(config.brokerId())) {
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

                // If fetched by follower replica, update replication progress and advance HW
                if (replicaId != null && state != null && state.isLeader(config.brokerId())) {
                    state.updateFollowerFetch(replicaId, fetchOffset, partitionLog.logEndOffset(), config.replicaLagTimeMaxMs());
                    long newHw = state.computeIsrHighWatermark(partitionLog.logEndOffset());
                    partitionLog.updateHighWatermark(newHw);
                    state.checkDelayedProduces(newHw);
                }

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
        isrExpirationScheduler.shutdownNow();
    }
}
