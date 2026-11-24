package io.pulsemq.consensus;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterMetadataControllerTest {

    private RaftNode raftNode;
    private ClusterMetadataStateMachine stateMachine;
    private ClusterMetadataController controller;

    @BeforeEach
    void setUp() throws Exception {
        stateMachine = new ClusterMetadataStateMachine();
        raftNode = new RaftNode(1, Collections.emptyMap(), stateMachine);

        // Wait for single-node election
        long deadline = System.currentTimeMillis() + 3000;
        while (raftNode.role() != RaftRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        // 300ms heartbeat timeout for fast test failure detection
        controller = new ClusterMetadataController(raftNode, stateMachine, 300L);
    }

    @AfterEach
    void tearDown() {
        if (controller != null) controller.close();
        if (raftNode != null) raftNode.close();
    }

    @Test
    void testTopicCreationAndReplicaAssignment() throws Exception {
        // Register 3 brokers
        controller.registerBroker(1, "127.0.0.1", 9091).get(2, TimeUnit.SECONDS);
        controller.registerBroker(2, "127.0.0.1", 9092).get(2, TimeUnit.SECONDS);
        controller.registerBroker(3, "127.0.0.1", 9093).get(2, TimeUnit.SECONDS);

        assertThat(stateMachine.currentImage().brokers()).hasSize(3);

        // Create topic with 3 partitions and replication factor 2
        ErrorCode code = controller.createTopic("orders", 3, (short) 2).get(3, TimeUnit.SECONDS);
        assertThat(code).isEqualTo(ErrorCode.NONE);

        MetadataImage image = stateMachine.currentImage();
        assertThat(image.topics()).containsKey("orders");

        MetadataImage.TopicImage topic = image.topics().get("orders");
        assertThat(topic.partitions()).hasSize(3);

        for (int p = 0; p < 3; p++) {
            MetadataImage.PartitionImage part = topic.partitions().get(p);
            assertThat(part).isNotNull();
            assertThat(part.replicas()).hasSize(2);
            assertThat(part.isr()).hasSize(2);
            assertThat(part.leaderEpoch()).isEqualTo(0);
            assertThat(part.leaderId()).isIn(1, 2, 3);
        }
    }

    @Test
    void testAutomatedPartitionLeaderFailover() throws Exception {
        // Register brokers 1 and 2
        controller.registerBroker(1, "127.0.0.1", 9091).get(2, TimeUnit.SECONDS);
        controller.registerBroker(2, "127.0.0.1", 9092).get(2, TimeUnit.SECONDS);

        // Explicitly propose a partition where Broker 2 is the leader and Broker 1 is follower
        TopicPartition tp = new TopicPartition("payments", 0);
        MetadataRecord.TopicRecord tr = new MetadataRecord.TopicRecord("payments", "id-123");
        raftNode.propose(MetadataRecord.serialize(tr)).get(2, TimeUnit.SECONDS);

        MetadataRecord.PartitionRecord pr = new MetadataRecord.PartitionRecord(
                "payments", 0, 2, 0, List.of(2, 1), List.of(2, 1)
        );
        raftNode.propose(MetadataRecord.serialize(pr)).get(2, TimeUnit.SECONDS);

        MetadataImage.PartitionImage initPart = stateMachine.currentImage().getPartition(tp);
        assertThat(initPart.leaderId()).isEqualTo(2);
        assertThat(initPart.leaderEpoch()).isEqualTo(0);

        // Send a heartbeat for broker 2 initially
        controller.recordHeartbeat(2);

        // Wait for broker 2's heartbeat to expire (>300ms)
        Thread.sleep(450);

        // Trigger or await failure detection check
        controller.checkBrokerFailuresAndElectLeaders();

        // Wait for Raft proposal and state machine application
        long deadline = System.currentTimeMillis() + 2000;
        MetadataImage.PartitionImage updated = stateMachine.currentImage().getPartition(tp);
        while (updated != null && updated.leaderId() == 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            updated = stateMachine.currentImage().getPartition(tp);
        }

        assertThat(updated).isNotNull();
        assertThat(updated.leaderId()).isEqualTo(1); // Broker 1 promoted to leader
        assertThat(updated.leaderEpoch()).isEqualTo(1); // Leader epoch incremented
        assertThat(updated.isr()).containsExactly(1); // Failed broker 2 removed from ISR
    }

    @Test
    void testDeleteTopic() throws Exception {
        controller.registerBroker(1, "127.0.0.1", 9091).get(2, TimeUnit.SECONDS);
        ErrorCode createCode = controller.createTopic("telemetry", 1, (short) 1).get(2, TimeUnit.SECONDS);
        assertThat(createCode).isEqualTo(ErrorCode.NONE);
        assertThat(stateMachine.currentImage().topics()).containsKey("telemetry");

        ErrorCode deleteCode = controller.deleteTopic("telemetry").get(2, TimeUnit.SECONDS);
        assertThat(deleteCode).isEqualTo(ErrorCode.NONE);
        assertThat(stateMachine.currentImage().topics()).doesNotContainKey("telemetry");
    }
}
