package io.pulsemq.broker;

import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.client.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class PulseEndToEndIntegrationTest {

    @TempDir
    Path tempDir;

    private PulseServer server;
    private PulseProducer producer;
    private PulseConsumer consumer;

    @BeforeEach
    void setUp() throws Exception {
        File dataDir = tempDir.resolve("pulse-data").toFile();
        BrokerConfig config = new BrokerConfig(
                1,
                "127.0.0.1",
                0, // random ephemeral port
                dataDir,
                1,
                1,
                1,
                10000L
        );

        server = new PulseServer(config);
        server.start();

        int port = server.boundPort();
        producer = new PulseProducer("127.0.0.1", port, "test-producer", (short) 1);
        consumer = new PulseConsumer("127.0.0.1", port, "integration-group", "test-consumer");
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) consumer.close();
        if (producer != null) producer.close();
        if (server != null) server.close();
    }

    @Test
    void testProduceAndConsumeEndToEnd() throws Exception {
        String topic = "telemetry";
        int messageCount = 10;

        // Send sequentially to verify deterministic ordered offsets
        for (int i = 0; i < messageCount; i++) {
            ProducerRecord record = new ProducerRecord(topic, "device-" + i, "temp=" + (20 + i));
            RecordMetadata meta = producer.send(record).get();
            assertThat(meta.offset()).isEqualTo(i);
            assertThat(meta.topicPartition().topic()).isEqualTo(topic);
        }

        // Consume published messages
        consumer.subscribe(topic);

        List<ConsumerRecord> received = new ArrayList<>();
        int attempts = 0;
        while (received.size() < messageCount && attempts++ < 10) {
            List<ConsumerRecord> batch = consumer.poll(Duration.ofMillis(500));
            received.addAll(batch);
        }

        assertThat(received).hasSize(messageCount);
        for (int i = 0; i < messageCount; i++) {
            ConsumerRecord r = received.get(i);
            assertThat(r.offset()).isEqualTo(i);
            assertThat(r.keyAsString()).isEqualTo("device-" + i);
            assertThat(r.valueAsString()).isEqualTo("temp=" + (20 + i));
        }

        // Commit offsets
        consumer.commitSync();
    }
}
