package io.pulsemq.broker;

import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.client.RecordMetadata;
import io.pulsemq.client.benchmark.ConsumerPerformance;
import io.pulsemq.client.benchmark.LatencyStats;
import io.pulsemq.client.benchmark.ProducerPerformance;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PulseEndToEndStressTest {

    @TempDir
    Path tempDir;

    private PulseServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        File dataDir = tempDir.resolve("stress-data").toFile();
        BrokerConfig config = new BrokerConfig(
                1, "127.0.0.1", 0, dataDir, 4, 1, 1, 10000L
        );
        server = new PulseServer(config);
        server.start();
        port = server.boundPort();

        // Await leader controller readiness
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
    void testConcurrentHighThroughputProduceAndConsumeStress() throws Exception {
        String topic = "stress-stream";
        int numPartitions = 4;
        int totalRecords = 20_000;
        int workerThreads = 8;
        int recordsPerWorker = totalRecords / workerThreads;

        // Pre-create 4-partition topic
        server.metadataController().createTopic(topic, numPartitions, (short) 1).get(5, TimeUnit.SECONDS);

        LatencyStats producerStats = new LatencyStats(totalRecords);
        List<CompletableFuture<Void>> workerFutures = new ArrayList<>();
        Map<Integer, Set<Long>> producedOffsetsPerPartition = new ConcurrentHashMap<>();
        for (int p = 0; p < numPartitions; p++) {
            producedOffsetsPerPartition.put(p, ConcurrentHashMap.newKeySet());
        }

        long startProduce = System.nanoTime();

        // 1. Concurrent virtual thread load generators
        for (int w = 0; w < workerThreads; w++) {
            final int workerId = w;
            CompletableFuture<Void> wf = new CompletableFuture<>();
            workerFutures.add(wf);

            Thread.ofVirtual().name("stress-worker-" + w).start(() -> {
                try (PulseProducer producer = new PulseProducer("127.0.0.1", port, "stress-prod-" + workerId, (short) 1, 32 * 1024, 2L)) {
                    List<CompletableFuture<RecordMetadata>> batchFutures = new ArrayList<>(recordsPerWorker);

                    for (int i = 0; i < recordsPerWorker; i++) {
                        int rId = workerId * recordsPerWorker + i;
                        int partition = rId % numPartitions;
                        String key = "stress-k-" + rId;
                        byte[] val = ("stress-payload-val-" + rId).getBytes();

                        long sendNanos = System.nanoTime();
                        ProducerRecord record = new ProducerRecord(topic, partition, key, val);
                        CompletableFuture<RecordMetadata> future = producer.send(record);

                        future.whenComplete((meta, err) -> {
                            long lat = System.nanoTime() - sendNanos;
                            if (err == null) {
                                producerStats.record(lat, val.length);
                                producedOffsetsPerPartition.get(meta.topicPartition().partition()).add(meta.offset());
                            }
                        });

                        batchFutures.add(future);
                    }

                    producer.flush();
                    CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0])).join();
                    wf.complete(null);
                } catch (Throwable t) {
                    wf.completeExceptionally(t);
                }
            });
        }

        CompletableFuture.allOf(workerFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

        LatencyStats.Snapshot produceSnapshot = producerStats.snapshot();
        produceSnapshot.printSummary("PulseMQ 20,000 Records Concurrent Stress Benchmark");

        assertThat(produceSnapshot.totalRecords()).isEqualTo(totalRecords);
        assertThat(produceSnapshot.recordsPerSec()).isGreaterThan(1000.0);

        // Verify partition offset count totals
        int totalProduced = 0;
        for (int p = 0; p < numPartitions; p++) {
            totalProduced += producedOffsetsPerPartition.get(p).size();
        }
        assertThat(totalProduced).isEqualTo(totalRecords);

        // 2. Multi-partition consumer drain and verification
        try (PulseConsumer consumer = new PulseConsumer("127.0.0.1", port, "stress-consumer-group", "stress-reader")) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (int p = 0; p < numPartitions; p++) {
                partitions.add(new TopicPartition(topic, p));
            }
            consumer.assign(partitions);

            Map<Integer, List<ConsumerRecord>> consumedByPartition = new HashMap<>();
            for (int p = 0; p < numPartitions; p++) {
                consumedByPartition.put(p, new ArrayList<>());
            }

            int consumedTotal = 0;
            int emptyPolls = 0;
            long startConsume = System.nanoTime();

            while (consumedTotal < totalRecords && emptyPolls < 20) {
                List<ConsumerRecord> polled = consumer.poll(Duration.ofMillis(300));
                if (polled.isEmpty()) {
                    emptyPolls++;
                    Thread.sleep(20);
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord cr : polled) {
                    consumedByPartition.get(cr.partition()).add(cr);
                }
                consumedTotal += polled.size();
            }

            long consumeTimeNanos = System.nanoTime() - startConsume;
            double consumeSec = consumeTimeNanos / 1_000_000_000.0;
            double consumeRate = consumedTotal / consumeSec;

            System.out.printf("Consumer Drain Complete: %,d records in %.2f s (%,.2f records/sec)%n",
                    consumedTotal, consumeSec, consumeRate);

            // 3. Strict Data Integrity Assertions
            assertThat(consumedTotal).isEqualTo(totalRecords);

            for (int p = 0; p < numPartitions; p++) {
                List<ConsumerRecord> list = consumedByPartition.get(p);
                assertThat(list).isNotEmpty();

                // Verify monotonic continuous offsets
                long expectedOffset = 0L;
                for (ConsumerRecord cr : list) {
                    assertThat(cr.offset()).isEqualTo(expectedOffset);
                    assertThat(cr.keyAsString()).startsWith("stress-k-");
                    assertThat(cr.valueAsString()).startsWith("stress-payload-val-");
                    expectedOffset++;
                }
            }
        }
    }

    @Test
    void testProducerAndConsumerPerformanceToolsExecution() throws Exception {
        String topic = "tool-bench-topic";
        int records = 5_000;

        server.metadataController().createTopic(topic, 2, (short) 1).get(5, TimeUnit.SECONDS);

        // Run ProducerPerformance harness
        ProducerPerformance.BenchmarkConfig prodConfig = new ProducerPerformance.BenchmarkConfig(
                "127.0.0.1", port, topic, 2, records, 512, (short) 1, 32 * 1024, 2L, 4, 0
        );
        LatencyStats.Snapshot prodSnapshot = ProducerPerformance.run(prodConfig);
        assertThat(prodSnapshot.totalRecords()).isEqualTo(records);
        assertThat(prodSnapshot.p99LatencyMs()).isGreaterThan(0.0);

        // Run ConsumerPerformance harness
        ConsumerPerformance.BenchmarkConfig consConfig = new ConsumerPerformance.BenchmarkConfig(
                "127.0.0.1", port, topic, 2, records, Duration.ofMillis(500), "tool-cons-group"
        );
        LatencyStats.Snapshot consSnapshot = ConsumerPerformance.run(consConfig);
        assertThat(consSnapshot.totalRecords()).isGreaterThan(0);
        assertThat(consSnapshot.recordsPerSec()).isGreaterThan(0.0);
    }
}
