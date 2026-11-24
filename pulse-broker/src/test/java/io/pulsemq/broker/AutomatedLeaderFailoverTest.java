package io.pulsemq.broker;

import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.client.RecordMetadata;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.consensus.MetadataImage;
import io.pulsemq.consensus.MetadataRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class AutomatedLeaderFailoverTest {

    @TempDir
    Path tempDir;

    private PulseServer broker1;
    private PulseServer broker2;

    @BeforeEach
    void setUp() throws Exception {
        File dataDir1 = tempDir.resolve("broker-1-data").toFile();
        File dataDir2 = tempDir.resolve("broker-2-data").toFile();

        // Broker 1 (initial partition leader)
        BrokerConfig config1 = new BrokerConfig(
                1, "127.0.0.1", 0, dataDir1, 1, 2, 2, 10000L
        );
        broker1 = new PulseServer(config1);
        broker1.start();

        // Broker 2 (follower and failover candidate)
        BrokerConfig config2 = new BrokerConfig(
                2, "127.0.0.1", 0, dataDir2, 1, 2, 2, 10000L
        );
        broker2 = new PulseServer(config2);
        broker2.start();

        long deadline = System.currentTimeMillis() + 3000;
        while ((!broker1.metadataController().isActiveController() || !broker2.metadataController().isActiveController()) && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        // Register both brokers in state machines
        broker1.metadataController().registerBroker(1, "127.0.0.1", broker1.boundPort()).get(2, TimeUnit.SECONDS);
        broker1.metadataController().registerBroker(2, "127.0.0.1", broker2.boundPort()).get(2, TimeUnit.SECONDS);
        broker2.metadataController().registerBroker(1, "127.0.0.1", broker1.boundPort()).get(2, TimeUnit.SECONDS);
        broker2.metadataController().registerBroker(2, "127.0.0.1", broker2.boundPort()).get(2, TimeUnit.SECONDS);

        // Keep heartbeats alive initially
        broker1.metadataController().recordHeartbeat(1);
        broker1.metadataController().recordHeartbeat(2);
        broker2.metadataController().recordHeartbeat(1);
        broker2.metadataController().recordHeartbeat(2);

        // Connect follower fetcher so broker2 replicates from broker1
        broker2.replicaFetcherManager().registerPeer(1, "127.0.0.1", broker1.boundPort());
    }

    @AfterEach
    void tearDown() {
        if (broker2 != null) broker2.close();
        if (broker1 != null) broker1.close();
    }

    @Test
    void testBrokerLeaderCrashTriggersAutomatedFailoverAndZeroDataLoss() throws Exception {
        String topic = "failover-stream";
        TopicPartition tp = new TopicPartition(topic, 0);

        // Assign partition 0: Leader=Broker 1, Replicas=[1, 2], ISR=[1, 2]
        broker1.replicaManager().becomeLeaderOrFollower(tp, 1, 0, List.of(1, 2));
        broker2.replicaManager().becomeLeaderOrFollower(tp, 1, 0, List.of(1, 2));

        // Propose partition metadata to broker2's consensus engine so controller is aware of topology
        MetadataRecord.TopicRecord tr = new MetadataRecord.TopicRecord(topic, "top-1");
        broker2.raftNode().propose(MetadataRecord.serialize(tr)).get(2, TimeUnit.SECONDS);

        MetadataRecord.PartitionRecord pr = new MetadataRecord.PartitionRecord(
                topic, 0, 1, 0, List.of(1, 2), List.of(1, 2)
        );
        broker2.raftNode().propose(MetadataRecord.serialize(pr)).get(2, TimeUnit.SECONDS);

        // 1. Produce 3 messages to Broker 1 with acks=all (replicated to Broker 2)
        try (PulseProducer producer = new PulseProducer("127.0.0.1", broker1.boundPort(), "prod-1", (short) -1)) {
            for (int i = 0; i < 3; i++) {
                RecordMetadata rm = producer.send(new ProducerRecord(topic, "sensor-" + i, "data-" + i)).get(5, TimeUnit.SECONDS);
                assertThat(rm.offset()).isEqualTo(i);
            }
        }

        // Verify Broker 2 has replicated all 3 messages
        assertThat(broker2.logManager().getLog(tp).logEndOffset()).isEqualTo(3L);

        // 2. Simulate sudden CRASH of Broker 1
        broker1.close();
        broker2.metadataController().invalidateHeartbeat(1);

        // 3. Broker 2 detects Broker 1 is dead (heartbeat expired)
        broker2.metadataController().checkBrokerFailuresAndElectLeaders();

        // Wait for failover to complete on Broker 2
        long deadline = System.currentTimeMillis() + 3000;
        MetadataImage.PartitionImage partImg = broker2.stateMachine().currentImage().getPartition(tp);
        while (partImg != null && partImg.leaderId() != 2 && System.currentTimeMillis() < deadline) {
            broker2.metadataController().checkBrokerFailuresAndElectLeaders();
            Thread.sleep(100);
            partImg = broker2.stateMachine().currentImage().getPartition(tp);
        }

        assertThat(partImg).isNotNull();
        assertThat(partImg.leaderId()).isEqualTo(2); // Broker 2 promoted to leader!
        assertThat(partImg.leaderEpoch()).isGreaterThanOrEqualTo(1); // Leader epoch bumped

        // 4. Verify Broker 2's local ReplicaManager now acts as leader
        var localState = broker2.replicaManager().allPartitions().get(tp);
        assertThat(localState).isNotNull();
        assertThat(localState.leaderId()).isEqualTo(2);
        assertThat(localState.isLeader(2)).isTrue();

        // 5. Produce a new message directly to promoted leader Broker 2
        try (PulseProducer producer2 = new PulseProducer("127.0.0.1", broker2.boundPort(), "prod-2", (short) 1)) {
            RecordMetadata rm = producer2.send(new ProducerRecord(topic, "sensor-post-failover", "data-failover")).get(5, TimeUnit.SECONDS);
            assertThat(rm.offset()).isEqualTo(3L);
        }

        // 6. Consume all 4 messages from Broker 2 to confirm zero message loss
        try (PulseConsumer consumer = new PulseConsumer("127.0.0.1", broker2.boundPort(), "failover-consumer-group")) {
            consumer.subscribe(topic);
            List<ConsumerRecord> records = new ArrayList<>();
            int attempts = 0;
            while (records.size() < 4 && attempts++ < 15) {
                records.addAll(consumer.poll(Duration.ofMillis(300)));
            }

            assertThat(records).hasSize(4);
            assertThat(records.get(0).keyAsString()).isEqualTo("sensor-0");
            assertThat(records.get(1).keyAsString()).isEqualTo("sensor-1");
            assertThat(records.get(2).keyAsString()).isEqualTo("sensor-2");
            assertThat(records.get(3).keyAsString()).isEqualTo("sensor-post-failover");
            assertThat(records.get(3).offset()).isEqualTo(3L);
        }
    }
}
