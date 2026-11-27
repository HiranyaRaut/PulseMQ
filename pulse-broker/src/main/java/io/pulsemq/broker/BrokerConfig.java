package io.pulsemq.broker;

import java.io.File;

/**
 * Broker configuration parameters.
 */
public record BrokerConfig(
        int brokerId,
        String host,
        int port,
        File logDir,
        int defaultPartitions,
        int defaultReplicationFactor,
        int minInsyncReplicas,
        long replicaLagTimeMaxMs,
        boolean metricsEnabled,
        int metricsPort
) {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_PARTITIONS = 1;
    public static final int DEFAULT_REPLICATION_FACTOR = 1;
    public static final int DEFAULT_MIN_INSYNC_REPLICAS = 1;
    public static final long DEFAULT_REPLICA_LAG_TIME_MAX_MS = 10000L;
    public static final boolean DEFAULT_METRICS_ENABLED = true;
    public static final int DEFAULT_METRICS_PORT = 9090;

    public BrokerConfig(
            int brokerId,
            String host,
            int port,
            File logDir,
            int defaultPartitions,
            int defaultReplicationFactor,
            int minInsyncReplicas,
            long replicaLagTimeMaxMs
    ) {
        this(brokerId, host, port, logDir, defaultPartitions, defaultReplicationFactor, minInsyncReplicas,
                replicaLagTimeMaxMs, DEFAULT_METRICS_ENABLED, port == 0 ? 0 : DEFAULT_METRICS_PORT);
    }

    public static BrokerConfig createDefault(int brokerId, File logDir) {
        return new BrokerConfig(
                brokerId,
                DEFAULT_HOST,
                DEFAULT_PORT,
                logDir,
                DEFAULT_PARTITIONS,
                DEFAULT_REPLICATION_FACTOR,
                DEFAULT_MIN_INSYNC_REPLICAS,
                DEFAULT_REPLICA_LAG_TIME_MAX_MS
        );
    }
}
