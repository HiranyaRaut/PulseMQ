package io.pulsemq.client.benchmark;

/**
 * Unified CLI harness runner for PulseMQ benchmarks.
 * <p>
 * Supported modes:
 * <ul>
 *   <li><code>producer</code>: Run multi-threaded producer throughput & latency benchmark</li>
 *   <li><code>consumer</code>: Run high-throughput consumer drain benchmark</li>
 *   <li><code>e2e</code>: Run end-to-end produce-to-consume round-trip latency benchmark</li>
 * </ul>
 */
public class PulseBenchmarkRunner {

    public static void main(String[] args) throws Exception {
        String mode = getArg(args, "--mode", "producer");
        String host = getArg(args, "--host", "127.0.0.1");
        int port = Integer.parseInt(getArg(args, "--port", "9092"));
        String topic = getArg(args, "--topic", "pulse-benchmark");
        int records = Integer.parseInt(getArg(args, "--records", "100000"));
        int recordSize = Integer.parseInt(getArg(args, "--record-size", "1024"));
        short acks = Short.parseShort(getArg(args, "--acks", "1"));
        int batchSize = Integer.parseInt(getArg(args, "--batch-size", "65536"));
        long lingerMs = Long.parseLong(getArg(args, "--linger-ms", "5"));
        int threads = Integer.parseInt(getArg(args, "--threads", String.valueOf(Runtime.getRuntime().availableProcessors())));
        int partitions = Integer.parseInt(getArg(args, "--partitions", "1"));
        int targetThroughput = Integer.parseInt(getArg(args, "--target-throughput", "0"));

        System.out.println("Starting PulseMQ Benchmark in mode: " + mode.toUpperCase());

        switch (mode.toLowerCase()) {
            case "producer" -> {
                ProducerPerformance.BenchmarkConfig config = new ProducerPerformance.BenchmarkConfig(
                        host, port, topic, partitions, records, recordSize, acks, batchSize, lingerMs, threads, targetThroughput
                );
                LatencyStats.Snapshot snapshot = ProducerPerformance.run(config);
                snapshot.printSummary("PulseMQ Producer Performance Results");
            }
            case "consumer" -> {
                ConsumerPerformance.BenchmarkConfig config = new ConsumerPerformance.BenchmarkConfig(
                        host, port, topic, partitions, records, java.time.Duration.ofMillis(500), "cli-bench-group"
                );
                LatencyStats.Snapshot snapshot = ConsumerPerformance.run(config);
                snapshot.printSummary("PulseMQ Consumer Performance Results");
            }
            case "e2e" -> {
                EndToEndLatencyBenchmark.BenchmarkConfig config = new EndToEndLatencyBenchmark.BenchmarkConfig(
                        host, port, topic, Math.min(10000, records), acks
                );
                LatencyStats.Snapshot snapshot = EndToEndLatencyBenchmark.run(config);
                snapshot.printSummary("PulseMQ End-To-End Latency Results");
            }
            default -> System.err.println("Unknown benchmark mode: " + mode + " (use 'producer', 'consumer', or 'e2e')");
        }
    }

    private static String getArg(String[] args, String flag, String defaultVal) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equalsIgnoreCase(flag)) {
                return args[i + 1];
            }
        }
        return defaultVal;
    }
}
