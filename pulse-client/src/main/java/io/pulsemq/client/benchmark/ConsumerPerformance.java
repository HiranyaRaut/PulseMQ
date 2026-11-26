package io.pulsemq.client.benchmark;

import io.pulsemq.client.ConsumerRecord;
import io.pulsemq.client.PulseConsumer;
import io.pulsemq.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * High-performance consumer drain benchmark for PulseConsumer.
 * Measures maximum fetch throughput (records/sec, MB/sec) and zero-copy read rates.
 */
public class ConsumerPerformance {

    private static final Logger log = LoggerFactory.getLogger(ConsumerPerformance.class);

    public record BenchmarkConfig(
            String host,
            int port,
            String topic,
            int numPartitions,
            int totalRecordsToConsume,
            Duration pollTimeout,
            String groupId
    ) {
        public static BenchmarkConfig createDefault(String host, int port, String topic, int totalRecordsToConsume) {
            return new BenchmarkConfig(
                    host,
                    port,
                    topic,
                    1,
                    totalRecordsToConsume,
                    Duration.ofMillis(500),
                    "bench-consumer-group"
            );
        }
    }

    public static LatencyStats.Snapshot run(BenchmarkConfig config) throws Exception {
        LatencyStats stats = new LatencyStats(config.totalRecordsToConsume());

        try (PulseConsumer consumer = new PulseConsumer(config.host(), config.port(), config.groupId(), "bench-consumer-1")) {
            if (config.numPartitions() > 1) {
                List<TopicPartition> partitions = new ArrayList<>();
                for (int p = 0; p < config.numPartitions(); p++) {
                    partitions.add(new TopicPartition(config.topic(), p));
                }
                consumer.assign(partitions);
            } else {
                consumer.subscribe(config.topic());
            }

            int consumed = 0;
            int emptyPolls = 0;

            while (consumed < config.totalRecordsToConsume() && emptyPolls < 30) {
                long startPoll = System.nanoTime();
                List<ConsumerRecord> batch = consumer.poll(config.pollTimeout());
                long elapsedPoll = System.nanoTime() - startPoll;

                if (batch.isEmpty()) {
                    emptyPolls++;
                    Thread.sleep(10);
                    continue;
                }

                emptyPolls = 0;
                long batchBytes = 0;
                for (ConsumerRecord r : batch) {
                    batchBytes += (r.key() != null ? r.key().length : 0) + (r.value() != null ? r.value().length : 0);
                }

                stats.record(elapsedPoll, batchBytes);
                consumed += batch.size();
            }

            consumer.commitSync();
            return stats.snapshot();
        }
    }
}
