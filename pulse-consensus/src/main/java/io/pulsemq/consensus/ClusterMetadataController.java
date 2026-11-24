package io.pulsemq.consensus;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KRaft-style Metadata Controller managing cluster metadata, broker liveness,
 * topic lifecycle, and automated partition leader failover via Raft consensus.
 */
public class ClusterMetadataController implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClusterMetadataController.class);

    private final RaftNode raftNode;
    private final ClusterMetadataStateMachine stateMachine;
    private final long brokerHeartbeatTimeoutMs;

    private final Map<Integer, Long> brokerHeartbeats = new ConcurrentHashMap<>();
    private final AtomicInteger partitionAssignmentCounter = new AtomicInteger(0);
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> failureDetectionTask;
    private volatile boolean running = true;

    public ClusterMetadataController(
            RaftNode raftNode,
            ClusterMetadataStateMachine stateMachine,
            long brokerHeartbeatTimeoutMs
    ) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
        this.brokerHeartbeatTimeoutMs = brokerHeartbeatTimeoutMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pulse-metadata-controller-" + raftNode.nodeId());
            t.setDaemon(true);
            return t;
        });

        startFailureDetection();
    }

    public ClusterMetadataController(RaftNode raftNode, ClusterMetadataStateMachine stateMachine) {
        this(raftNode, stateMachine, 5000L);
    }

    public RaftNode raftNode() {
        return raftNode;
    }

    public ClusterMetadataStateMachine stateMachine() {
        return stateMachine;
    }

    public boolean isActiveController() {
        return raftNode.role() == RaftRole.LEADER;
    }

    public int activeControllerId() {
        return raftNode.currentLeaderId();
    }

    private void startFailureDetection() {
        failureDetectionTask = scheduler.scheduleWithFixedDelay(
                this::checkBrokerFailuresAndElectLeaders,
                500, 500, TimeUnit.MILLISECONDS
        );
    }

    /**
     * Record broker heartbeat to maintain liveness.
     */
    public void recordHeartbeat(int brokerId) {
        brokerHeartbeats.put(brokerId, System.currentTimeMillis());
    }

    /**
     * Invalidate broker heartbeat (for failure simulation or testing).
     */
    public void invalidateHeartbeat(int brokerId) {
        brokerHeartbeats.put(brokerId, 0L);
    }

    public boolean isBrokerAlive(int brokerId) {
        if (brokerId == raftNode.nodeId()) {
            return true;
        }
        Long last = brokerHeartbeats.get(brokerId);
        if (last == null) {
            return false;
        }
        return (System.currentTimeMillis() - last) <= brokerHeartbeatTimeoutMs;
    }

    public List<Integer> getAliveBrokerIds() {
        MetadataImage image = stateMachine.currentImage();
        List<Integer> alive = new ArrayList<>();
        for (Integer brokerId : image.brokers().keySet()) {
            if (isBrokerAlive(brokerId)) {
                alive.add(brokerId);
            }
        }
        if (alive.isEmpty() && isBrokerAlive(raftNode.nodeId())) {
            alive.add(raftNode.nodeId());
        }
        return alive;
    }

    /**
     * Registers a broker with the metadata cluster.
     */
    public CompletableFuture<Void> registerBroker(int brokerId, String host, int port) {
        recordHeartbeat(brokerId);
        MetadataRecord record = new MetadataRecord.RegisterBrokerRecord(
                brokerId, host, port, System.currentTimeMillis(), false
        );
        byte[] data = MetadataRecord.serialize(record);

        if (!isActiveController()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Not active controller, current leader is: " + raftNode.currentLeaderId())
            );
        }

        return raftNode.propose(data).thenApply(idx -> null);
    }

    /**
     * Proposes dynamic creation of a topic with partitioned replica placement.
     */
    public CompletableFuture<ErrorCode> createTopic(String topicName, int numPartitions, short replicationFactor) {
        if (!isActiveController()) {
            return CompletableFuture.completedFuture(ErrorCode.NOT_LEADER_OR_FOLLOWER);
        }

        MetadataImage image = stateMachine.currentImage();
        if (image.topics().containsKey(topicName)) {
            return CompletableFuture.completedFuture(ErrorCode.TOPIC_ALREADY_EXISTS);
        }

        List<Integer> aliveBrokers = getAliveBrokerIds();
        if (aliveBrokers.isEmpty() || replicationFactor > aliveBrokers.size() || replicationFactor <= 0) {
            return CompletableFuture.completedFuture(ErrorCode.INVALID_REPLICATION_FACTOR);
        }

        String topicId = UUID.randomUUID().toString();
        MetadataRecord.TopicRecord topicRecord = new MetadataRecord.TopicRecord(topicName, topicId);
        byte[] topicData = MetadataRecord.serialize(topicRecord);

        return raftNode.propose(topicData).thenCompose(tIdx -> {
            List<CompletableFuture<Long>> partitionFutures = new ArrayList<>();

            for (int p = 0; p < numPartitions; p++) {
                int startIdx = Math.abs(partitionAssignmentCounter.getAndIncrement()) % aliveBrokers.size();
                List<Integer> replicas = new ArrayList<>();
                for (int r = 0; r < replicationFactor; r++) {
                    replicas.add(aliveBrokers.get((startIdx + r) % aliveBrokers.size()));
                }
                int leader = replicas.get(0);
                List<Integer> isr = new ArrayList<>(replicas);

                MetadataRecord.PartitionRecord partRecord = new MetadataRecord.PartitionRecord(
                        topicName, p, leader, 0, replicas, isr
                );
                partitionFutures.add(raftNode.propose(MetadataRecord.serialize(partRecord)));
            }

            return CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> ErrorCode.NONE);
        }).exceptionally(ex -> {
            log.error("Failed to commit topic creation for {}", topicName, ex);
            return ErrorCode.UNKNOWN_SERVER_ERROR;
        });
    }

    /**
     * Proposes deletion of a topic.
     */
    public CompletableFuture<ErrorCode> deleteTopic(String topicName) {
        if (!isActiveController()) {
            return CompletableFuture.completedFuture(ErrorCode.NOT_LEADER_OR_FOLLOWER);
        }

        MetadataImage image = stateMachine.currentImage();
        if (!image.topics().containsKey(topicName)) {
            return CompletableFuture.completedFuture(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
        }

        MetadataRecord.RemoveTopicRecord removeRecord = new MetadataRecord.RemoveTopicRecord(topicName);
        return raftNode.propose(MetadataRecord.serialize(removeRecord))
                .thenApply(idx -> ErrorCode.NONE)
                .exceptionally(ex -> {
                    log.error("Failed to commit topic deletion for {}", topicName, ex);
                    return ErrorCode.UNKNOWN_SERVER_ERROR;
                });
    }

    /**
     * Inspects partitions whose leaders have failed, promotes next alive in-sync replica,
     * and proposes PartitionChangeRecord via Raft consensus.
     */
    public void checkBrokerFailuresAndElectLeaders() {
        if (!running || !isActiveController()) {
            return;
        }

        MetadataImage image = stateMachine.currentImage();
        for (MetadataImage.TopicImage topic : image.topics().values()) {
            for (MetadataImage.PartitionImage partition : topic.partitions().values()) {
                int currentLeader = partition.leaderId();
                if (!isBrokerAlive(currentLeader)) {
                    electNewPartitionLeader(partition);
                }
            }
        }
    }

    private void electNewPartitionLeader(MetadataImage.PartitionImage partition) {
        TopicPartition tp = partition.topicPartition();
        int deadLeader = partition.leaderId();

        // 1. Choose from alive ISR replicas
        Optional<Integer> newLeaderOpt = partition.isr().stream()
                .filter(id -> id != deadLeader && isBrokerAlive(id))
                .findFirst();

        // 2. Fall back to any alive replica
        if (newLeaderOpt.isEmpty()) {
            newLeaderOpt = partition.replicas().stream()
                    .filter(id -> id != deadLeader && isBrokerAlive(id))
                    .findFirst();
        }

        if (newLeaderOpt.isEmpty()) {
            log.warn("No alive replica available for leader failover on {}", tp);
            return;
        }

        int newLeader = newLeaderOpt.get();
        int newEpoch = partition.leaderEpoch() + 1;
        List<Integer> newIsr = partition.isr().stream()
                .filter(this::isBrokerAlive)
                .toList();
        if (newIsr.isEmpty()) {
            newIsr = List.of(newLeader);
        }

        log.info("Controller [{}] initiating leader failover for {}: dead leader={}, new leader={}, new epoch={}, new ISR={}",
                raftNode.nodeId(), tp, deadLeader, newLeader, newEpoch, newIsr);

        MetadataRecord.PartitionChangeRecord changeRecord = new MetadataRecord.PartitionChangeRecord(
                tp.topic(), tp.partition(), newLeader, newEpoch, newIsr
        );

        raftNode.propose(MetadataRecord.serialize(changeRecord)).exceptionally(ex -> {
            log.error("Failed to propose PartitionChangeRecord for {}", tp, ex);
            return null;
        });
    }

    @Override
    public void close() {
        running = false;
        if (failureDetectionTask != null) {
            failureDetectionTask.cancel(false);
        }
        scheduler.shutdownNow();
    }
}
