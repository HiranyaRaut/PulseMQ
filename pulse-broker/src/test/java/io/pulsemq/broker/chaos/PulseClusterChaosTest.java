package io.pulsemq.broker.chaos;

import io.pulsemq.broker.PulseServer;
import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.client.RecordMetadata;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.consensus.MetadataImage;
import io.pulsemq.consensus.RaftRole;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 5 Multi-Broker Chaos & Fault-Injection Engine Test Suite.
 * Validates automated partition leader failover, dynamic follower crash & ISR catchup,
 * active KRaft metadata controller crash & election failover, and zero-loss continuous workload.
 */
class PulseClusterChaosTest {

    private static final Logger log = LoggerFactory.getLogger(PulseClusterChaosTest.class);

    @TempDir
    Path tempDir;

    private ChaosCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        // 3-node cluster with 1500ms replica lag timeout for fast chaos failure detection
        cluster = new ChaosCluster(tempDir, 3, 1500L);
        cluster.start();
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @DisplayName("Scenario 1: Abrupt partition leader kill under continuous workload with automated failover and 100% data integrity")
    void testAbruptPartitionLeaderKillUnderContinuousWorkload() throws Exception {
        String topic = "chaos-failover-stream";
        TopicPartition tp = new TopicPartition(topic, 0);

        // 1. Create replicated topic with replication factor = 3 across all 3 nodes
        cluster.createTopic(topic, 1, (short) 3);

        PulseServer initialLeader = cluster.waitForLeader(tp, Duration.ofSeconds(5));
        assertThat(initialLeader).isNotNull();
        int initialLeaderId = initialLeader.config().brokerId();
        log.info("Initial partition leader for {} is Broker [{}]", tp, initialLeaderId);

        // 2. Start continuous workload generator publishing with acks=all
        try (ContinuousWorkload workload = new ContinuousWorkload(cluster, topic, 0, (short) -1, 15L)) {
            workload.start();

            // Wait until at least 10 records are acknowledged
            long deadline = System.currentTimeMillis() + 5000;
            while (workload.acknowledgedCount() < 10 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            int preCrashAcknowledged = workload.acknowledgedCount();
            assertThat(preCrashAcknowledged).isGreaterThanOrEqualTo(10);
            log.info("Pre-crash acknowledged count: {}", preCrashAcknowledged);

            // 3. Abruptly kill the partition leader mid-stream
            log.warn("CHAOS INJECTION: Killing active leader Broker [{}]...", initialLeaderId);
            cluster.killBroker(initialLeaderId);

            // 4. Wait for cluster to elect a new leader among surviving brokers
            PulseServer newLeader = cluster.waitForLeader(tp, Duration.ofSeconds(5));
            assertThat(newLeader).isNotNull();
            int newLeaderId = newLeader.config().brokerId();
            assertThat(newLeaderId).isNotEqualTo(initialLeaderId);
            log.info("New partition leader elected: Broker [{}] (previous was [{}])", newLeaderId, initialLeaderId);

            // 5. Allow workload to continue producing to the new leader
            deadline = System.currentTimeMillis() + 5000;
            while (workload.acknowledgedCount() < preCrashAcknowledged + 10 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            int totalAcknowledged = workload.acknowledgedCount();
            assertThat(totalAcknowledged).isGreaterThan(preCrashAcknowledged);
            log.info("Post-failover acknowledged count: {}", totalAcknowledged);

            // 6. Stop workload and verify 100% data integrity
            ContinuousWorkload.IntegrityReport report = workload.verifyDataIntegrity(Duration.ofSeconds(6));

            assertThat(report.verifiedCount()).isGreaterThanOrEqualTo(report.acknowledgedCount());
            assertThat(report.isMonotonic()).isTrue();
            assertThat(report.hasZeroGaps()).isTrue();
            assertThat(report.lastOffset()).isEqualTo(report.verifiedCount() - 1);
            log.info("Scenario 1 verified successfully: {} records delivered with zero loss and strict monotonic ordering.",
                    report.verifiedCount());
        }
    }

    @Test
    @DisplayName("Scenario 2: Follower crash, dynamic ISR eviction, restart, log catchup, and ISR re-admission")
    void testFollowerCrashRestartAndIsrCatchup() throws Exception {
        String topic = "chaos-isr-stream";
        TopicPartition tp = new TopicPartition(topic, 0);

        cluster.createTopic(topic, 1, (short) 3);

        PulseServer leader = cluster.waitForLeader(tp, Duration.ofSeconds(5));
        assertThat(leader).isNotNull();
        int leaderId = leader.config().brokerId();

        // Select a follower to crash
        int followerId = (leaderId == 1) ? 2 : 1;
        PulseServer follower = cluster.broker(followerId);
        log.info("Partition leader is Broker [{}], follower to test is Broker [{}]", leaderId, followerId);

        // 1. Produce 5 initial messages with acks=all
        try (PulseProducer producer = new PulseProducer("127.0.0.1", leader.boundPort(), "test-prod", (short) -1)) {
            for (int i = 0; i < 5; i++) {
                RecordMetadata rm = producer.send(new ProducerRecord(topic, "key-" + i, "data-" + i))
                        .get(5, TimeUnit.SECONDS);
                assertThat(rm.offset()).isEqualTo(i);
            }
        }

        // Wait for follower to replicate all 5 records
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            var fLog = follower.logManager().getLog(tp);
            if (fLog != null && fLog.logEndOffset() >= 5L) {
                break;
            }
            Thread.sleep(50);
        }
        assertThat(follower.logManager().getLog(tp).logEndOffset()).isEqualTo(5L);

        // 2. Kill follower broker
        log.warn("CHAOS INJECTION: Killing follower Broker [{}]...", followerId);
        cluster.killBroker(followerId);

        // Wait past replicaLagTimeMaxMs (1500ms) and trigger ISR check on leader
        Thread.sleep(1700);
        leader.replicaManager().checkIsrExpirations();

        // Verify leader evicted dead follower from ISR
        var partState = leader.replicaManager().allPartitions().get(tp);
        assertThat(partState).isNotNull();
        assertThat(partState.isr()).doesNotContain(followerId);
        log.info("Dead follower [{}] successfully evicted from ISR. Current ISR: {}", followerId, partState.isr());

        // 3. Produce 5 more messages while follower is DEAD
        try (PulseProducer producer = new PulseProducer("127.0.0.1", leader.boundPort(), "test-prod-during-crash", (short) -1)) {
            for (int i = 5; i < 10; i++) {
                RecordMetadata rm = producer.send(new ProducerRecord(topic, "key-" + i, "data-" + i))
                        .get(5, TimeUnit.SECONDS);
                assertThat(rm.offset()).isEqualTo(i);
            }
        }
        assertThat(leader.logManager().getLog(tp).logEndOffset()).isEqualTo(10L);

        // 4. Revive follower broker
        log.info("CHAOS RECOVERY: Restarting follower Broker [{}]...", followerId);
        PulseServer restartedFollower = cluster.restartBroker(followerId);
        assertThat(restartedFollower.isRunning()).isTrue();

        // 5. Follower's ReplicaFetcherManager catches up from offset 5 to 10
        deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            var rfLog = restartedFollower.logManager().getLog(tp);
            if (rfLog != null && rfLog.logEndOffset() >= 10L) {
                break;
            }
            Thread.sleep(50);
        }

        var restartedLog = restartedFollower.logManager().getLog(tp);
        assertThat(restartedLog).isNotNull();
        assertThat(restartedLog.logEndOffset()).isEqualTo(10L);
        log.info("Restarted follower [{}] successfully caught up to leader LEO (10).", followerId);

        // 6. Verify leader re-admitted caught-up follower back into ISR
        deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            if (partState.isr().contains(followerId)) {
                break;
            }
            Thread.sleep(50);
        }
        assertThat(partState.isr()).contains(followerId);
        log.info("Follower [{}] dynamically re-admitted to ISR. Final ISR: {}", followerId, partState.isr());

        // 7. Consume all 10 records directly from revived follower to prove disk consistency
        try (PulseConsumer consumer = new PulseConsumer("127.0.0.1", restartedFollower.boundPort(), "follower-verifier")) {
            consumer.assign(tp);
            List<ConsumerRecord> records = new ArrayList<>();
            deadline = System.currentTimeMillis() + 3000;
            while (records.size() < 10 && System.currentTimeMillis() < deadline) {
                records.addAll(consumer.poll(Duration.ofMillis(200)));
            }

            assertThat(records).hasSize(10);
            for (int i = 0; i < 10; i++) {
                ConsumerRecord cr = records.get(i);
                assertThat(cr.offset()).isEqualTo(i);
                assertThat(cr.keyAsString()).isEqualTo("key-" + i);
                assertThat(cr.valueAsString()).isEqualTo("data-" + i);
            }
        }
    }

    @Test
    @DisplayName("Scenario 3: Active KRaft metadata controller crash, quorum failover, and metadata continuity")
    void testActiveControllerCrashAndQuorumFailover() throws Exception {
        // 1. Identify active controller
        PulseServer initialController = cluster.waitForActiveController(Duration.ofSeconds(5));
        int controllerId = initialController.config().brokerId();
        log.info("Initial KRaft active controller is Broker [{}]", controllerId);

        // Pre-create topic on initial controller
        cluster.createTopic("pre-controller-crash-topic", 1, (short) 2);

        // 2. Abruptly kill active controller
        log.warn("CHAOS INJECTION: Killing active KRaft controller Broker [{}]...", controllerId);
        cluster.killBroker(controllerId);

        // 3. Surviving 2 nodes hold quorum and elect a new controller
        long deadline = System.currentTimeMillis() + 5000;
        PulseServer newController = null;
        while (System.currentTimeMillis() < deadline) {
            for (int id : cluster.activeBrokers()) {
                PulseServer b = cluster.broker(id);
                if (b != null && b.isRunning() && b.raftNode().role() == RaftRole.LEADER) {
                    newController = b;
                    break;
                }
            }
            if (newController != null) break;
            Thread.sleep(50);
        }

        assertThat(newController).isNotNull();
        int newControllerId = newController.config().brokerId();
        assertThat(newControllerId).isNotEqualTo(controllerId);
        log.info("New KRaft controller elected: Broker [{}] (previous was [{}])", newControllerId, controllerId);

        // 4. Create new topic using the new controller
        String postCrashTopic = "post-controller-crash-topic";
        cluster.createTopic(postCrashTopic, 1, (short) 2);

        TopicPartition tp = new TopicPartition(postCrashTopic, 0);
        PulseServer topicLeader = cluster.waitForLeader(tp, Duration.ofSeconds(5));
        assertThat(topicLeader).isNotNull();
        log.info("Leader for post-crash topic is Broker [{}]", topicLeader.config().brokerId());

        // 5. Produce and consume records on post-crash topic
        try (PulseProducer producer = new PulseProducer("127.0.0.1", topicLeader.boundPort(), "ctrl-prod", (short) 1)) {
            for (int i = 0; i < 3; i++) {
                RecordMetadata rm = producer.send(new ProducerRecord(postCrashTopic, "k-" + i, "v-" + i))
                        .get(5, TimeUnit.SECONDS);
                assertThat(rm.offset()).isEqualTo(i);
            }
        }

        try (PulseConsumer consumer = new PulseConsumer("127.0.0.1", topicLeader.boundPort(), "ctrl-cons")) {
            consumer.assign(tp);
            List<ConsumerRecord> records = new ArrayList<>();
            deadline = System.currentTimeMillis() + 3000;
            while (records.size() < 3 && System.currentTimeMillis() < deadline) {
                records.addAll(consumer.poll(Duration.ofMillis(200)));
            }
            assertThat(records).hasSize(3);
        }

        // 6. Revive original controller node and verify it rejoins as follower
        log.info("CHAOS RECOVERY: Restarting original controller Broker [{}]...", controllerId);
        PulseServer revivedNode = cluster.restartBroker(controllerId);
        assertThat(revivedNode.isRunning()).isTrue();

        // Verify revived node catches up metadata image containing the new topic
        deadline = System.currentTimeMillis() + 4000;
        MetadataImage img = revivedNode.stateMachine().currentImage();
        while ((img == null || img.topics().get(postCrashTopic) == null) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            img = revivedNode.stateMachine().currentImage();
        }

        assertThat(img).isNotNull();
        assertThat(img.topics().get(postCrashTopic)).isNotNull();
        log.info("Revived Broker [{}] caught up metadata containing topic '{}'", controllerId, postCrashTopic);
    }
}
