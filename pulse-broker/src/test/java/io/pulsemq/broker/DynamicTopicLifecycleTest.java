package io.pulsemq.broker;

import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.NetworkClient;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.protocol.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicTopicLifecycleTest {

    @TempDir
    Path tempDir;

    private PulseServer server;
    private NetworkClient client;

    @BeforeEach
    void setUp() throws Exception {
        File dataDir = tempDir.resolve("broker-data").toFile();
        BrokerConfig config = new BrokerConfig(
                1, "127.0.0.1", 0, dataDir, 1, 1, 1, 10000L
        );
        server = new PulseServer(config);
        server.start();

        long deadline = System.currentTimeMillis() + 3000;
        while (!server.metadataController().isActiveController() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        client = new NetworkClient("127.0.0.1", server.boundPort());
        client.connect();
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void testDynamicTopicCreationMetadataAndDeletion() throws Exception {
        String topic = "dynamic-orders";

        // 1. Create Topic via binary wire protocol
        RequestHeader createHeader = new RequestHeader(
                ApiKeys.CREATE_TOPICS, (short) 1, client.nextCorrelationId(), "admin-client"
        );
        CreateTopicsRequest createReq = new CreateTopicsRequest(
                createHeader,
                Map.of(topic, new CreateTopicsRequest.TopicDetails(2, (short) 1)),
                5000
        );

        PulseResponse createResp = client.send(createReq).get(5, TimeUnit.SECONDS);
        assertThat(createResp).isInstanceOf(CreateTopicsResponse.class);
        CreateTopicsResponse topicResp = (CreateTopicsResponse) createResp;
        assertThat(topicResp.results()).containsEntry(topic, ErrorCode.NONE);

        // 2. Query Metadata to verify partitions and leader assignment
        RequestHeader metaHeader = new RequestHeader(
                ApiKeys.METADATA, (short) 1, client.nextCorrelationId(), "meta-client"
        );
        MetadataRequest metaReq = new MetadataRequest(metaHeader, List.of(topic));
        MetadataResponse metaResp = (MetadataResponse) client.send(metaReq).get(5, TimeUnit.SECONDS);

        assertThat(metaResp.topicMetadata()).hasSize(1);
        MetadataResponse.TopicMetadata tm = metaResp.topicMetadata().get(0);
        assertThat(tm.topic()).isEqualTo(topic);
        assertThat(tm.errorCode()).isEqualTo(ErrorCode.NONE);
        assertThat(tm.partitions()).hasSize(2);
        for (MetadataResponse.PartitionMetadata pm : tm.partitions()) {
            assertThat(pm.leaderId()).isEqualTo(1);
            assertThat(pm.replicas()).contains(1);
            assertThat(pm.isr()).contains(1);
        }

        // 3. Produce and Consume from the dynamically created topic
        try (PulseProducer producer = new PulseProducer("127.0.0.1", server.boundPort(), "dyn-producer", (short) 1);
             PulseConsumer consumer = new PulseConsumer("127.0.0.1", server.boundPort(), "dyn-group", "dyn-consumer")) {

            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord(topic, "k" + i, "v" + i)).get(5, TimeUnit.SECONDS);
            }

            consumer.subscribe(topic);
            List<ConsumerRecord> records = new ArrayList<>();
            int attempts = 0;
            while (records.size() < 5 && attempts++ < 10) {
                records.addAll(consumer.poll(Duration.ofMillis(300)));
            }
            assertThat(records).hasSize(5);
        }

        // 4. Delete Topic via binary protocol
        RequestHeader delHeader = new RequestHeader(
                ApiKeys.DELETE_TOPICS, (short) 1, client.nextCorrelationId(), "admin-client"
        );
        DeleteTopicsRequest delReq = new DeleteTopicsRequest(delHeader, List.of(topic), 5000);
        DeleteTopicsResponse delResp = (DeleteTopicsResponse) client.send(delReq).get(5, TimeUnit.SECONDS);
        assertThat(delResp.results()).containsEntry(topic, ErrorCode.NONE);

        // 5. Verify topic is removed from metadata
        assertThat(server.stateMachine().currentImage().topics()).doesNotContainKey(topic);
    }
}
