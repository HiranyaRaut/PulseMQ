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
        long replicaLagTimeMaxMs
) {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_PARTITIONS = 1;
    public static final int DEFAULT_REPLICATION_FACTOR = 1;
    public static final int DEFAULT_MIN_INSYNC_REPLICAS = 1;
    public static final long DEFAULT_REPLICA_LAG_TIME_MAX_MS = 10000L;

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
