package io.pulsemq.client.benchmark;

import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseProducer;
import io.pulsemq.client.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance multi-threaded load generator and throughput benchmark for PulseProducer.
 * Measures sustained ingestion rate (records/sec, MB/sec) and latency percentiles under high concurrency.
 */
public class ProducerPerformance {

    private static final Logger log = LoggerFactory.getLogger(ProducerPerformance.class);

    public record BenchmarkConfig(
            String host,
            int port,
            String topic,
            int numPartitions,
            int numRecords,
            int recordSizeBytes,
            short acks,
            int batchSize,
            long lingerMs,
            int producerThreads,
            int targetThroughput
    ) {
        public static BenchmarkConfig createDefault(String host, int port, String topic, int numRecords) {
            return new BenchmarkConfig(
                    host,
                    port,
                    topic,
                    1,
                    numRecords,
                    1024, // 1KB record size
                    (short) 1,
                    64 * 1024, // 64KB batch size
                    5L, // 5ms linger
                    Math.max(2, Runtime.getRuntime().availableProcessors()),
                    0 // unthrottled
            );
        }
    }

    public static LatencyStats.Snapshot run(BenchmarkConfig config) throws Exception {
        byte[] payload = new byte[config.recordSizeBytes()];
        new Random(42).nextBytes(payload);

        LatencyStats stats = new LatencyStats(config.numRecords());
        int recordsPerThread = config.numRecords() / config.producerThreads();
        int remainingRecords = config.numRecords() % config.producerThreads();

        List<PulseProducer> producers = new ArrayList<>();
        List<CompletableFuture<Void>> threadFutures = new ArrayList<>();
        AtomicInteger recordCounter = new AtomicInteger(0);

        try {
            for (int t = 0; t < config.producerThreads(); t++) {
                int threadRecords = recordsPerThread + (t == 0 ? remainingRecords : 0);
                String clientId = "bench-producer-" + t;

                PulseProducer producer = new PulseProducer(
                        config.host(),
                        config.port(),
                        clientId,
                        config.acks(),
                        config.batchSize(),
                        config.lingerMs()
                );
                producers.add(producer);

                CompletableFuture<Void> threadFuture = new CompletableFuture<>();
                threadFutures.add(threadFuture);

                Thread.ofVirtual().name("producer-worker-" + t).start(() -> {
                    List<CompletableFuture<RecordMetadata>> sendFutures = new ArrayList<>(threadRecords);
                    try {
                        for (int i = 0; i < threadRecords; i++) {
                            int rId = recordCounter.getAndIncrement();
                            int partition = config.numPartitions() > 1 ? (rId % config.numPartitions()) : 0;
                            String key = "key-" + rId;

                            long sendTime = System.nanoTime();
                            ProducerRecord record = new ProducerRecord(config.topic(), partition, key, payload);
                            CompletableFuture<RecordMetadata> sendFuture = producer.send(record);

                            sendFuture.whenComplete((meta, err) -> {
                                long latency = System.nanoTime() - sendTime;
                                if (err == null) {
                                    stats.record(latency, payload.length);
                                } else {
                                    log.error("Produce failed", err);
                                }
                            });

                            sendFutures.add(sendFuture);

                            // Optional rate-limiting throttle
                            if (config.targetThroughput() > 0) {
                                double targetPerThread = (double) config.targetThroughput() / config.producerThreads();
                                long delayNanos = (long) (1_000_000_000.0 / targetPerThread);
                                if (delayNanos > 0) {
                                    long waitNanos = sendTime + delayNanos - System.nanoTime();
                                    if (waitNanos > 100_000) {
                                        Thread.sleep(waitNanos / 1_000_000, (int) (waitNanos % 1_000_000));
                                    }
                                }
                            }
                        }

                        producer.flush();
                        CompletableFuture.allOf(sendFutures.toArray(new CompletableFuture[0])).join();
                        threadFuture.complete(null);
                    } catch (Throwable t1) {
                        log.error("Producer worker encountered error", t1);
                        threadFuture.completeExceptionally(t1);
                    }
                });
            }

            // Await all workers
            CompletableFuture.allOf(threadFutures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);

            // Final flush
            for (PulseProducer p : producers) {
                p.flush();
            }

            return stats.snapshot();
        } finally {
            for (PulseProducer p : producers) {
                p.close();
            }
        }
    }
}
