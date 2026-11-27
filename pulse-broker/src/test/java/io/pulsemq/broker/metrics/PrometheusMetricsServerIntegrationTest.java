package io.pulsemq.broker.metrics;

import io.pulsemq.broker.BrokerConfig;
import io.pulsemq.broker.PulseServer;
import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusMetricsServerIntegrationTest {

    @TempDir
    Path tempDir;

    private PulseServer server;
    private int brokerPort;
    private int metricsPort;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() throws Exception {
        File dataDir = tempDir.resolve("metrics-broker-data").toFile();
        // port=0 and metricsPort=0 for dynamic ephemeral port allocation
        BrokerConfig config = new BrokerConfig(
                1, "127.0.0.1", 0, dataDir, 2, 1, 1, 10000L, true, 0
        );

        server = new PulseServer(config);
        server.start();
        brokerPort = server.boundPort();
        metricsPort = server.metricsPort();
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        // Wait for Raft controller readiness
        long deadline = System.currentTimeMillis() + 3000;
        while (!server.metadataController().isActiveController() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void testPrometheusMetricsScrapeAndHealthEndpoints() throws Exception {
        String topic = "telemetry-metrics";
        server.metadataController().createTopic(topic, 2, (short) 1).get(5, TimeUnit.SECONDS);

        // 1. Ingest records with PulseProducer
        try (PulseProducer producer = new PulseProducer("127.0.0.1", brokerPort, "metrics-producer", (short) 1)) {
            for (int i = 0; i < 50; i++) {
                producer.send(new ProducerRecord(topic, 0, "k-" + i, ("val-" + i).getBytes())).join();
                producer.send(new ProducerRecord(topic, 1, "k-" + i, ("val-" + i).getBytes())).join();
            }
        }

        // 2. Consume a subset of records with PulseConsumer to establish consumer group lag
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        String groupId = "metrics-test-group";

        try (PulseConsumer consumer = new PulseConsumer("127.0.0.1", brokerPort, groupId, "client-worker")) {
            consumer.assign(List.of(tp0, tp1));
            List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records).isNotEmpty();

            // Commit only 20 records on partition 0 (creating 30 records of lag: LEO 50 - committed 20 = 30)
            consumer.commitSync(java.util.Map.of(tp0, 20L));
        }

        // 3. Query /metrics endpoint
        HttpRequest metricsReq = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + metricsPort + "/metrics"))
                .GET()
                .build();

        HttpResponse<String> metricsResp = httpClient.send(metricsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(metricsResp.statusCode()).isEqualTo(200);
        assertThat(metricsResp.headers().firstValue("Content-Type").orElse(""))
                .contains("text/plain");

        String body = metricsResp.body();

        // Verify Ingestion & Egress metrics
        assertThat(body)
                .contains("# HELP pulsemq_messages_in_total")
                .contains("# TYPE pulsemq_messages_in_total counter")
                .contains("pulsemq_messages_in_total{topic=\"telemetry-metrics\"} 100")
                .contains("pulsemq_produce_requests_total 100");

        // Verify Partition LEO, HW, and Segment metrics
        assertThat(body)
                .contains("pulsemq_partition_log_end_offset{partition=\"0\",topic=\"telemetry-metrics\"} 50")
                .contains("pulsemq_partition_log_end_offset{partition=\"1\",topic=\"telemetry-metrics\"} 50")
                .contains("pulsemq_partition_high_watermark{partition=\"0\",topic=\"telemetry-metrics\"} 50")
                .contains("pulsemq_partition_segments_count{partition=\"0\",topic=\"telemetry-metrics\"} 1");

        // Verify Consumer Group Committed Offset and Lag
        assertThat(body)
                .contains("pulsemq_consumer_group_committed_offset{group=\"metrics-test-group\",partition=\"0\",topic=\"telemetry-metrics\"} 20")
                .contains("pulsemq_consumer_group_lag{group=\"metrics-test-group\",partition=\"0\",topic=\"telemetry-metrics\"} 30");

        // Verify Raft Consensus and Topology metrics
        assertThat(body)
                .contains("pulsemq_raft_current_term 1")
                .contains("pulsemq_raft_is_leader 1")
                .contains("pulsemq_cluster_brokers_count 1")
                .contains("pulsemq_cluster_topics_count 1")
                .contains("pulsemq_cluster_partitions_count 2")
                .contains("pulsemq_cluster_under_replicated_partitions 0");

        // Verify JVM metrics
        assertThat(body)
                .contains("pulsemq_jvm_heap_bytes_used")
                .contains("pulsemq_jvm_uptime_seconds");

        // 4. Query /healthz and /health endpoints
        HttpRequest healthReq = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + metricsPort + "/healthz"))
                .GET()
                .build();
        HttpResponse<String> healthResp = httpClient.send(healthReq, HttpResponse.BodyHandlers.ofString());
        assertThat(healthResp.statusCode()).isEqualTo(200);
        assertThat(healthResp.body())
                .contains("\"status\":\"UP\"")
                .contains("\"brokerId\":1");

        // 5. Query /readyz endpoint
        HttpRequest readyReq = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + metricsPort + "/readyz"))
                .GET()
                .build();
        HttpResponse<String> readyResp = httpClient.send(readyReq, HttpResponse.BodyHandlers.ofString());
        assertThat(readyResp.statusCode()).isEqualTo(200);
        assertThat(readyResp.body()).contains("OK");
    }
}
