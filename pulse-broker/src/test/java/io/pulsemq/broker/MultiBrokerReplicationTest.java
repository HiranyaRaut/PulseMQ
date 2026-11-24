package io.pulsemq.broker;

import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.client.RecordMetadata;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MultiBrokerReplicationTest {

    @TempDir
    Path tempDir;

    private PulseServer leaderBroker;
    private PulseServer followerBroker;
    private PulseProducer producer;
    private PulseConsumer followerConsumer;

    @BeforeEach
    void setUp() throws Exception {
        File leaderDataDir = tempDir.resolve("leader-data").toFile();
        File followerDataDir = tempDir.resolve("follower-data").toFile();

        // Leader broker (brokerId = 1)
        BrokerConfig leaderConfig = new BrokerConfig(
                1, "127.0.0.1", 0, leaderDataDir, 1, 2, 2, 10000L
        );
        leaderBroker = new PulseServer(leaderConfig);
        leaderBroker.start();

        // Follower broker (brokerId = 2)
        BrokerConfig followerConfig = new BrokerConfig(
                2, "127.0.0.1", 0, followerDataDir, 1, 2, 2, 10000L
        );
        followerBroker = new PulseServer(followerConfig);
        followerBroker.start();

        TopicPartition tp = new TopicPartition("cluster-stream", 0);
        List<Integer> replicas = List.of(1, 2);

        // Configure partition metadata on both brokers
        leaderBroker.replicaManager().becomeLeaderOrFollower(tp, 1, 1, replicas);
        followerBroker.replicaManager().becomeLeaderOrFollower(tp, 1, 1, replicas);

        // Register leader broker in follower's fetcher
        followerBroker.replicaFetcherManager().registerPeer(1, "127.0.0.1", leaderBroker.boundPort());

        // Producer connected to leader with acks = all (-1)
        producer = new PulseProducer("127.0.0.1", leaderBroker.boundPort(), "leader-producer", (short) -1);

        // Consumer connected directly to follower broker
        followerConsumer = new PulseConsumer("127.0.0.1", followerBroker.boundPort(), "follower-reader-group");
    }

    @AfterEach
    void tearDown() {
        if (followerConsumer != null) followerConsumer.close();
        if (producer != null) producer.close();
        if (followerBroker != null) followerBroker.close();
        if (leaderBroker != null) leaderBroker.close();
    }

    @Test
    void testAcksAllReplicationAndFollowerConsumption() throws Exception {
        String topic = "cluster-stream";
        int messageCount = 5;

        // Publish with acks=all: should only succeed once replicated to follower
        for (int i = 0; i < messageCount; i++) {
            ProducerRecord record = new ProducerRecord(topic, "sensor-" + i, "reading=" + (100 + i));
            RecordMetadata meta = producer.send(record).get();
            assertThat(meta.offset()).isEqualTo(i);
        }

        // Verify follower log contains all records
        TopicPartition tp = new TopicPartition(topic, 0);
        var followerLog = followerBroker.logManager().getLog(tp);
        assertThat(followerLog).isNotNull();
        assertThat(followerLog.logEndOffset()).isEqualTo(messageCount);

        // Read directly from follower broker
        followerConsumer.subscribe(topic);
        List<ConsumerRecord> received = new ArrayList<>();
        int attempts = 0;
        while (received.size() < messageCount && attempts++ < 10) {
            List<ConsumerRecord> batch = followerConsumer.poll(Duration.ofMillis(300));
            received.addAll(batch);
        }

        assertThat(received).hasSize(messageCount);
        for (int i = 0; i < messageCount; i++) {
            ConsumerRecord r = received.get(i);
            assertThat(r.offset()).isEqualTo(i);
            assertThat(r.keyAsString()).isEqualTo("sensor-" + i);
            assertThat(r.valueAsString()).isEqualTo("reading=" + (100 + i));
        }
    }
}
