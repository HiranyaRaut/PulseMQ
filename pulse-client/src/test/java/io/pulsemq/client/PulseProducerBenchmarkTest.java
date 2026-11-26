package io.pulsemq.client;

import io.pulsemq.client.benchmark.LatencyStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PulseProducerBenchmarkTest {

    @Test
    void testLatencyStatsAccuratePercentilesAndThroughput() {
        LatencyStats stats = new LatencyStats(10_000);

        // Record 1,000 samples with varying latencies: 1ms to 100ms
        for (int i = 1; i <= 1000; i++) {
            long latencyNanos = i * 1_000_000L; // 1ms, 2ms, ..., 1000ms
            stats.record(latencyNanos, 1024); // 1KB per record
        }

        LatencyStats.Snapshot snapshot = stats.snapshot();

        assertThat(snapshot.totalRecords()).isEqualTo(1000);
        assertThat(snapshot.totalBytes()).isEqualTo(1000 * 1024);
        assertThat(snapshot.minLatencyMs()).isBetween(0.9, 1.1);
        assertThat(snapshot.maxLatencyMs()).isBetween(990.0, 1001.0);
        assertThat(snapshot.p50LatencyMs()).isBetween(495.0, 505.0);
        assertThat(snapshot.p95LatencyMs()).isBetween(945.0, 955.0);
        assertThat(snapshot.p99LatencyMs()).isBetween(985.0, 995.0);
        assertThat(snapshot.p999LatencyMs()).isBetween(998.0, 1001.0);
        assertThat(snapshot.recordsPerSec()).isGreaterThan(0.0);
        assertThat(snapshot.mbPerSec()).isGreaterThan(0.0);

        // Verify summary printing runs without error
        snapshot.printSummary("Unit Test Latency Stats");
    }
}
