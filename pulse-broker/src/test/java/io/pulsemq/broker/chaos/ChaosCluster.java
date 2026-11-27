package io.pulsemq.broker.chaos;

import io.pulsemq.broker.BrokerConfig;
import io.pulsemq.broker.PulseServer;
import io.pulsemq.broker.ReplicaManager;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.consensus.ClusterMetadataController;
import io.pulsemq.consensus.MetadataImage;
import io.pulsemq.consensus.RaftNode;
import io.pulsemq.consensus.RaftPeer;
import io.pulsemq.consensus.RaftRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Multi-Broker Chaos and Fault-Injection Cluster Harness.
 * Manages N PulseServer nodes, isolated storage directories, dynamic port binding,
 * in-memory Raft peer routing, automated partition replication, and failure simulation.
 */
public class ChaosCluster implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ChaosCluster.class);

    private final Path rootDir;
    private final int nodeCount;
    private final long replicaLagTimeMaxMs;
    private final Map<Integer, BrokerConfig> configs = new ConcurrentHashMap<>();
    private final Map<Integer, PulseServer> brokers = new ConcurrentHashMap<>();
    private final Map<Integer, Map<Integer, RaftPeer>> raftPeerMaps = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Raft peer implementation that forwards RPCs directly to the target node in the cluster.
     * When the target node is crashed/stopped, RPCs gracefully fail/reject without hanging.
     */
    private static class ClusterRaftPeer implements RaftPeer {
        private final int peerId;
        private final ChaosCluster cluster;

        public ClusterRaftPeer(int peerId, ChaosCluster cluster) {
            this.peerId = peerId;
            this.cluster = cluster;
        }

        @Override
        public int peerId() {
            return peerId;
        }

        @Override
        public CompletableFuture<RaftRpc.RequestVoteReply> requestVote(RaftRpc.RequestVoteArgs args) {
            PulseServer target = cluster.broker(peerId);
            if (target == null || !target.isRunning()) {
                return CompletableFuture.completedFuture(new RaftRpc.RequestVoteReply(args.term(), false));
            }
            return CompletableFuture.supplyAsync(() -> target.raftNode().handleRequestVote(args));
        }

        @Override
        public CompletableFuture<RaftRpc.AppendEntriesReply> appendEntries(RaftRpc.AppendEntriesArgs args) {
            PulseServer target = cluster.broker(peerId);
            if (target == null || !target.isRunning()) {
                return CompletableFuture.completedFuture(new RaftRpc.AppendEntriesReply(args.term(), false, 0));
            }
            return CompletableFuture.supplyAsync(() -> target.raftNode().handleAppendEntries(args));
        }
    }

    public ChaosCluster(Path rootDir, int nodeCount, long replicaLagTimeMaxMs) {
        this.rootDir = rootDir;
        this.nodeCount = nodeCount;
        this.replicaLagTimeMaxMs = replicaLagTimeMaxMs;
    }

    public ChaosCluster(Path rootDir, int nodeCount) {
        this(rootDir, nodeCount, 2500L);
    }

    /**
     * Initializes and starts all brokers in the cluster.
     */
    public synchronized void start() throws Exception {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        log.info("Starting ChaosCluster with {} nodes in {}", nodeCount, rootDir);

        // 1. Prepare configs and Raft peer maps
        for (int i = 1; i <= nodeCount; i++) {
            File dataDir = rootDir.resolve("broker-" + i + "-data").toFile();
            if (!dataDir.exists()) {
                dataDir.mkdirs();
            }

            BrokerConfig config = new BrokerConfig(
                    i,
                    "127.0.0.1",
                    0, // Dynamic ephemeral port
                    dataDir,
                    1,
                    Math.min(nodeCount, 3),
                    2,
                    replicaLagTimeMaxMs
            );
            configs.put(i, config);

            Map<Integer, RaftPeer> peerMap = new ConcurrentHashMap<>();
            for (int p = 1; p <= nodeCount; p++) {
                if (p != i) {
                    peerMap.put(p, new ClusterRaftPeer(p, this));
                }
            }
            raftPeerMaps.put(i, peerMap);
        }

        // 2. Start all brokers
        for (int i = 1; i <= nodeCount; i++) {
            PulseServer server = new PulseServer(configs.get(i), raftPeerMaps.get(i));
            server.start();
            brokers.put(i, server);
            log.info("Broker [{}] started on port {}", i, server.boundPort());
        }

        // 3. Connect replica fetchers between all nodes
        connectReplicaFetchers();

        // 4. Wait for Raft controller election
        PulseServer controller = waitForActiveController(Duration.ofSeconds(10));
        log.info("Active KRaft controller elected: Broker [{}]", controller.config().brokerId());

        // 5. Register all brokers in active controller
        for (PulseServer b : brokers.values()) {
            controller.metadataController().registerBroker(
                    b.config().brokerId(),
                    b.config().host(),
                    b.boundPort()
            ).get(5, TimeUnit.SECONDS);
            controller.metadataController().recordHeartbeat(b.config().brokerId());
        }

        log.info("ChaosCluster initialization complete. Active nodes: {}", activeBrokers());
    }

    private void connectReplicaFetchers() {
        for (PulseServer source : brokers.values()) {
            if (!source.isRunning()) continue;
            for (PulseServer target : brokers.values()) {
                if (!target.isRunning()) continue;
                if (source.config().brokerId() != target.config().brokerId()) {
                    source.replicaFetcherManager().registerPeer(
                            target.config().brokerId(),
                            target.config().host(),
                            target.boundPort()
                    );
                }
            }
        }
    }

    /**
     * Creates a topic and partitions across the cluster, waiting for metadata consensus propagation.
     */
    public void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
        PulseServer controller = activeController();
        if (controller == null) {
            controller = waitForActiveController(Duration.ofSeconds(5));
        }

        ErrorCode code = controller.metadataController().createTopic(topic, partitions, replicationFactor)
                .get(5, TimeUnit.SECONDS);
        if (code != ErrorCode.NONE && code != ErrorCode.TOPIC_ALREADY_EXISTS) {
            throw new IllegalStateException("Failed to create topic: " + topic + ", error=" + code);
        }

        // Wait for all live brokers to learn about the new topic
        long deadline = System.currentTimeMillis() + 5000;
        for (PulseServer b : brokers.values()) {
            if (!b.isRunning()) continue;
            while (System.currentTimeMillis() < deadline) {
                MetadataImage img = b.stateMachine().currentImage();
                if (img.topics().get(topic) != null && img.topics().get(topic).partitions().size() >= partitions) {
                    break;
                }
                Thread.sleep(50);
            }
        }
    }

    /**
     * Abruptly kills a broker simulating an immediate ungraceful crash.
     */
    public synchronized void killBroker(int brokerId) {
        PulseServer server = brokers.get(brokerId);
        if (server == null || !server.isRunning()) {
            return;
        }

        log.info("CHAOS: Killing broker [{}] immediately...", brokerId);
        server.close();

        // Invalidate heartbeats on surviving controllers
        for (PulseServer s : brokers.values()) {
            if (s.isRunning()) {
                s.metadataController().invalidateHeartbeat(brokerId);
                s.metadataController().checkBrokerFailuresAndElectLeaders();
            }
        }
    }

    /**
     * Restarts a previously crashed broker on the same data directory,
     * recovering existing commit logs, rejoining Raft consensus, and reconnecting replication.
     */
    public synchronized PulseServer restartBroker(int brokerId) throws Exception {
        log.info("CHAOS: Restarting broker [{}]...", brokerId);
        PulseServer existing = brokers.get(brokerId);
        if (existing != null && existing.isRunning()) {
            existing.close();
        }

        BrokerConfig config = configs.get(brokerId);
        PulseServer newServer = new PulseServer(config, raftPeerMaps.get(brokerId));
        newServer.start();
        brokers.put(brokerId, newServer);

        log.info("Broker [{}] restarted on port {}", brokerId, newServer.boundPort());

        // Connect fetchers between new server and live servers
        connectReplicaFetchers();

        // Register restarted broker with active controller
        PulseServer controller = activeController();
        if (controller != null) {
            try {
                controller.metadataController().registerBroker(
                        brokerId, config.host(), newServer.boundPort()
                ).get(3, TimeUnit.SECONDS);
                controller.metadataController().recordHeartbeat(brokerId);
            } catch (Exception e) {
                log.warn("Failed to register restarted broker {} with controller: {}", brokerId, e.getMessage());
            }
        }

        return newServer;
    }

    /**
     * Finds the broker that is currently the active leader for the specified partition.
     */
    public PulseServer leaderFor(TopicPartition tp) {
        for (PulseServer b : brokers.values()) {
            if (b.isRunning()) {
                ReplicaManager.PartitionState state = b.replicaManager().allPartitions().get(tp);
                if (state != null && state.isLeader(b.config().brokerId())) {
                    return b;
                }
            }
        }
        return null;
    }

    /**
     * Polls until an active leader is found for the given topic partition.
     */
    public PulseServer waitForLeader(TopicPartition tp, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            PulseServer leader = leaderFor(tp);
            if (leader != null && leader.isRunning()) {
                return leader;
            }
            // Trigger failure detection on running controllers to speed up failover
            for (PulseServer s : brokers.values()) {
                if (s.isRunning() && s.metadataController().isActiveController()) {
                    s.metadataController().checkBrokerFailuresAndElectLeaders();
                }
            }
            Thread.sleep(50);
        }
        return null;
    }

    /**
     * Polls until the partition's ISR matches the expected set.
     */
    public boolean waitForIsr(TopicPartition tp, Set<Integer> expectedIsr, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            PulseServer leader = leaderFor(tp);
            if (leader != null) {
                ReplicaManager.PartitionState state = leader.replicaManager().allPartitions().get(tp);
                if (state != null && new HashSet<>(state.isr()).equals(expectedIsr)) {
                    return true;
                }
                leader.replicaManager().checkIsrExpirations();
            }
            Thread.sleep(50);
        }
        return false;
    }

    /**
     * Returns the currently active KRaft controller node, or null if none.
     */
    public PulseServer activeController() {
        for (PulseServer b : brokers.values()) {
            if (b.isRunning() && b.metadataController().isActiveController()) {
                // Refresh heartbeats of live brokers on active controller
                for (PulseServer alive : brokers.values()) {
                    if (alive.isRunning()) {
                        b.metadataController().recordHeartbeat(alive.config().brokerId());
                    }
                }
                return b;
            }
        }
        return null;
    }

    /**
     * Waits for an active KRaft controller to be elected.
     */
    public PulseServer waitForActiveController(Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            PulseServer ctrl = activeController();
            if (ctrl != null) {
                return ctrl;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("No active controller elected within " + timeout);
    }

    /**
     * Triggers ISR expiration check on all active brokers.
     */
    public void triggerIsrExpirations() {
        for (PulseServer b : brokers.values()) {
            if (b.isRunning()) {
                b.replicaManager().checkIsrExpirations();
            }
        }
    }

    public PulseServer broker(int brokerId) {
        return brokers.get(brokerId);
    }

    public List<Integer> activeBrokers() {
        List<Integer> list = new ArrayList<>();
        for (Map.Entry<Integer, PulseServer> e : brokers.entrySet()) {
            if (e.getValue().isRunning()) {
                list.add(e.getKey());
            }
        }
        Collections.sort(list);
        return list;
    }

    public int nodeCount() {
        return nodeCount;
    }

    @Override
    public synchronized void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Closing ChaosCluster...");
        for (PulseServer b : brokers.values()) {
            try {
                b.close();
            } catch (Exception ignored) {}
        }
        brokers.clear();
        log.info("ChaosCluster closed.");
    }
}
