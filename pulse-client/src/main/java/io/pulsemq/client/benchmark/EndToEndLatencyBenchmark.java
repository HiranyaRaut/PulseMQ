package io.pulsemq.client.benchmark;

import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.ProducerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.client.PulseProducer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

/**
 * High-precision round-trip produce-to-consume latency profiler for PulseMQ.
 * Measures end-to-end traversal latencies across the TCP stack, broker virtual threads,
 * disk log append, and consumer fetch loops.
 */
public class EndToEndLatencyBenchmark {

    public record BenchmarkConfig(
            String host,
            int port,
            String topic,
            int numMessages,
            short acks
    ) {}

    public static LatencyStats.Snapshot run(BenchmarkConfig config) throws Exception {
        LatencyStats stats = new LatencyStats(config.numMessages());

        try (PulseProducer producer = new PulseProducer(config.host(), config.port(), "e2e-producer", config.acks());
             PulseConsumer consumer = new PulseConsumer(config.host(), config.port(), "e2e-consumer-group", "e2e-consumer")) {

            consumer.subscribe(config.topic());

            // Warm up consumer
            consumer.poll(Duration.ofMillis(100));

            for (int i = 0; i < config.numMessages(); i++) {
                long sendNanos = System.nanoTime();
                byte[] payload = ByteBuffer.allocate(16)
                        .putLong(i)
                        .putLong(sendNanos)
                        .array();

                producer.send(new ProducerRecord(config.topic(), "k-" + i, payload)).get();

                // Poll immediately to retrieve the published record
                boolean received = false;
                int attempts = 0;
                while (!received && attempts++ < 20) {
                    List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord r : records) {
                        if (r.value() != null && r.value().length >= 16) {
                            ByteBuffer buf = ByteBuffer.wrap(r.value());
                            long seq = buf.getLong();
                            long originalSendNanos = buf.getLong();
                            if (seq == i) {
                                long e2eNanos = System.nanoTime() - originalSendNanos;
                                stats.record(e2eNanos, r.value().length);
                                received = true;
                                break;
                            }
                        }
                    }
                }
            }

            return stats.snapshot();
        }
    }
}
