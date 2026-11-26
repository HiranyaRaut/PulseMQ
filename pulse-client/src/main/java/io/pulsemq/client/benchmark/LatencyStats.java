package io.pulsemq.client.benchmark;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * High-precision latency and throughput statistics tracker for PulseMQ benchmarks.
 * Computes exact percentiles (p50, p95, p99, p99.9, max) and data throughput.
 */
public class LatencyStats {

    private final long startNanos;
    private final LongAdder totalRecords = new LongAdder();
    private final LongAdder totalBytes = new LongAdder();
    private final AtomicLong minLatencyNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatencyNanos = new AtomicLong(0);

    // Dynamic latency samples array for exact percentile calculation
    private long[] latencies;
    private int sampleCount = 0;
    private final int maxSamples;
    private final Object lock = new Object();

    public LatencyStats(int maxSamples) {
        this.maxSamples = Math.max(10_000, maxSamples);
        this.latencies = new long[Math.min(100_000, this.maxSamples)];
        this.startNanos = System.nanoTime();
    }

    public LatencyStats() {
        this(500_000);
    }

    public void record(long latencyNanos, long bytes) {
        totalRecords.increment();
        totalBytes.add(bytes);

        minLatencyNanos.accumulateAndGet(latencyNanos, Math::min);
        maxLatencyNanos.accumulateAndGet(latencyNanos, Math::max);

        synchronized (lock) {
            if (sampleCount < maxSamples) {
                if (sampleCount >= latencies.length) {
                    latencies = Arrays.copyOf(latencies, Math.min(maxSamples, latencies.length * 2));
                }
                latencies[sampleCount++] = latencyNanos;
            }
        }
    }

    public Snapshot snapshot() {
        long endNanos = System.nanoTime();
        long durationNanos = Math.max(1, endNanos - startNanos);
        long count = totalRecords.sum();
        long bytes = totalBytes.sum();

        long minNanos = count > 0 ? minLatencyNanos.get() : 0;
        long maxNanos = maxLatencyNanos.get();

        long[] copy;
        int n;
        synchronized (lock) {
            n = sampleCount;
            copy = Arrays.copyOf(latencies, n);
        }

        Arrays.sort(copy);

        double p50 = percentile(copy, 0.50);
        double p95 = percentile(copy, 0.95);
        double p99 = percentile(copy, 0.99);
        double p999 = percentile(copy, 0.999);
        double avg = n > 0 ? average(copy) : 0.0;

        double durationSec = durationNanos / 1_000_000_000.0;
        double recordsPerSec = durationSec > 0 ? (count / durationSec) : 0;
        double mbPerSec = durationSec > 0 ? ((bytes / (1024.0 * 1024.0)) / durationSec) : 0;

        return new Snapshot(
                count,
                bytes,
                durationNanos,
                recordsPerSec,
                mbPerSec,
                minNanos / 1_000_000.0,
                maxNanos / 1_000_000.0,
                avg / 1_000_000.0,
                p50 / 1_000_000.0,
                p95 / 1_000_000.0,
                p99 / 1_000_000.0,
                p999 / 1_000_000.0
        );
    }

    private static double average(long[] array) {
        long sum = 0;
        for (long v : array) sum += v;
        return (double) sum / array.length;
    }

    private static double percentile(long[] sorted, double p) {
        if (sorted.length == 0) return 0.0;
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        idx = Math.max(0, Math.min(idx, sorted.length - 1));
        return sorted[idx];
    }

    public record Snapshot(
            long totalRecords,
            long totalBytes,
            long durationNanos,
            double recordsPerSec,
            double mbPerSec,
            double minLatencyMs,
            double maxLatencyMs,
            double avgLatencyMs,
            double p50LatencyMs,
            double p95LatencyMs,
            double p99LatencyMs,
            double p999LatencyMs
    ) {
        public void printSummary(String title) {
            double totalMb = totalBytes / (1024.0 * 1024.0);
            double durationMs = durationNanos / 1_000_000.0;

            System.out.println("================================================================================");
            System.out.printf("  %s%n", title);
            System.out.println("================================================================================");
            System.out.printf("  Total Records       : %,d records%n", totalRecords);
            System.out.printf("  Total Data Volume   : %.2f MB (%,d bytes)%n", totalMb, totalBytes);
            System.out.printf("  Elapsed Time        : %.2f ms (%.2f s)%n", durationMs, durationMs / 1000.0);
            System.out.printf("  Throughput          : %,.2f records/sec | %,.2f MB/sec%n", recordsPerSec, mbPerSec);
            System.out.println("--------------------------------------------------------------------------------");
            System.out.printf("  Latency Min         : %7.3f ms%n", minLatencyMs);
            System.out.printf("  Latency Avg         : %7.3f ms%n", avgLatencyMs);
            System.out.printf("  Latency p50 (Median): %7.3f ms%n", p50LatencyMs);
            System.out.printf("  Latency p95         : %7.3f ms%n", p95LatencyMs);
            System.out.printf("  Latency p99         : %7.3f ms%n", p99LatencyMs);
            System.out.printf("  Latency p99.9       : %7.3f ms%n", p999LatencyMs);
            System.out.printf("  Latency Max         : %7.3f ms%n", maxLatencyMs);
            System.out.println("================================================================================");
        }
    }
}
