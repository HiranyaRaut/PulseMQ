package io.pulsemq.broker.metrics;

import io.pulsemq.broker.GroupCoordinator;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.consensus.ClusterMetadataStateMachine;
import io.pulsemq.consensus.MetadataImage;
import io.pulsemq.consensus.RaftNode;
import io.pulsemq.storage.LogSegment;
import io.pulsemq.storage.LogManager;
import io.pulsemq.storage.PartitionLog;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Central metrics coordinator that aggregates broker throughput, storage, consumer group lag,
 * Raft consensus state, and JVM metrics for Prometheus exposition.
 */
public class BrokerMetrics {

    private final MetricsRegistry registry;
    private final LogManager logManager;
    private final GroupCoordinator groupCoordinator;
    private final ClusterMetadataStateMachine stateMachine;
    private final RaftNode raftNode;
    private final int brokerId;

    // Static counters
    private final Counter messagesInTotal;
    private final Counter bytesInTotal;
    private final Counter messagesOutTotal;
    private final Counter bytesOutTotal;
    private final Counter produceRequestsTotal;
    private final Counter fetchRequestsTotal;
    private final Counter produceErrorsTotal;

    // Active client connections gauge
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final Gauge activeConnectionsGauge;

    public BrokerMetrics(
            MetricsRegistry registry,
            LogManager logManager,
            GroupCoordinator groupCoordinator,
            ClusterMetadataStateMachine stateMachine,
            RaftNode raftNode,
            int brokerId
    ) {
        this.registry = registry;
        this.logManager = logManager;
        this.groupCoordinator = groupCoordinator;
        this.stateMachine = stateMachine;
        this.raftNode = raftNode;
        this.brokerId = brokerId;

        // Ingestion & Egress counters
        this.messagesInTotal = registry.counter("pulsemq_messages_in_total", "Total count of messages ingested into broker");
        this.bytesInTotal = registry.counter("pulsemq_bytes_in_total", "Total volume of bytes ingested into broker");
        this.messagesOutTotal = registry.counter("pulsemq_messages_out_total", "Total count of messages fetched by consumers");
        this.bytesOutTotal = registry.counter("pulsemq_bytes_out_total", "Total volume of bytes fetched by consumers");
        this.produceRequestsTotal = registry.counter("pulsemq_produce_requests_total", "Total number of produce requests received");
        this.fetchRequestsTotal = registry.counter("pulsemq_fetch_requests_total", "Total number of fetch requests received");
        this.produceErrorsTotal = registry.counter("pulsemq_produce_errors_total", "Total count of failed produce operations");

        // Active connections gauge
        this.activeConnectionsGauge = registry.gauge("pulsemq_active_connections", "Number of currently active client TCP connections");
        this.activeConnectionsGauge.set(0.0);

        // Register dynamic collectors
        registerPartitionMetrics();
        registerConsumerLagMetrics();
        registerConsensusAndTopologyMetrics();
        registerJvmMetrics();
    }

    public void recordMessageIn(String topic, int count, long bytes) {
        Map<String, String> labels = Map.of("topic", topic);
        messagesInTotal.inc(labels, count);
        bytesInTotal.inc(labels, bytes);
    }

    public void recordMessageOut(String topic, int count, long bytes) {
        Map<String, String> labels = Map.of("topic", topic);
        messagesOutTotal.inc(labels, count);
        bytesOutTotal.inc(labels, bytes);
    }

    public void recordProduceRequest() {
        produceRequestsTotal.inc();
    }

    public void recordProduceError() {
        produceErrorsTotal.inc();
    }

    public void recordFetchRequest() {
        fetchRequestsTotal.inc();
    }

    public void recordActiveConnection(int delta) {
        int current = activeConnections.addAndGet(delta);
        activeConnectionsGauge.set(Math.max(0, current));
    }

    public int activeConnectionsCount() {
        return activeConnections.get();
    }

    public MetricsRegistry registry() {
        return registry;
    }

    public int brokerId() {
        return brokerId;
    }

    private void registerPartitionMetrics() {
        registry.registerDynamicCollector(() -> {
            List<MetricSample> samples = new ArrayList<>();
            if (logManager == null) return samples;

            for (Map.Entry<TopicPartition, PartitionLog> entry : logManager.allLogs().entrySet()) {
                TopicPartition tp = entry.getKey();
                PartitionLog log = entry.getValue();

                Map<String, String> labels = Map.of(
                        "topic", tp.topic(),
                        "partition", String.valueOf(tp.partition())
                );

                long leo = log.logEndOffset();
                long hw = log.highWatermark();
                int segCount = log.segments().size();
                long sizeBytes = 0L;
                for (LogSegment seg : log.segments()) {
                    sizeBytes += seg.size();
                }

                samples.add(new MetricSample("pulsemq_partition_log_end_offset", labels, leo,
                        "Log end offset of partition", MetricType.GAUGE));
                samples.add(new MetricSample("pulsemq_partition_high_watermark", labels, hw,
                        "High watermark committed offset of partition", MetricType.GAUGE));
                samples.add(new MetricSample("pulsemq_partition_segments_count", labels, segCount,
                        "Number of segment files for partition on disk", MetricType.GAUGE));
                samples.add(new MetricSample("pulsemq_partition_size_bytes", labels, sizeBytes,
                        "Disk storage used by partition in bytes", MetricType.GAUGE));
            }
            return samples;
        });
    }

    private void registerConsumerLagMetrics() {
        registry.registerDynamicCollector(() -> {
            List<MetricSample> samples = new ArrayList<>();
            if (groupCoordinator == null || logManager == null) return samples;

            for (GroupCoordinator.GroupMetadata group : groupCoordinator.groups().values()) {
                String groupId = group.groupId();
                for (Map.Entry<TopicPartition, Long> entry : group.committedOffsets().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    long committedOffset = entry.getValue();

                    PartitionLog pLog = logManager.getLog(tp);
                    long leo = pLog != null ? pLog.logEndOffset() : committedOffset;
                    long lag = Math.max(0L, leo - committedOffset);

                    Map<String, String> labels = Map.of(
                            "group", groupId,
                            "topic", tp.topic(),
                            "partition", String.valueOf(tp.partition())
                    );

                    samples.add(new MetricSample("pulsemq_consumer_group_committed_offset", labels, committedOffset,
                            "Committed offset of consumer group partition", MetricType.GAUGE));
                    samples.add(new MetricSample("pulsemq_consumer_group_lag", labels, lag,
                            "Consumer group lag in records (LEO - committedOffset)", MetricType.GAUGE));
                }
            }
            return samples;
        });
    }

    private void registerConsensusAndTopologyMetrics() {
        registry.registerDynamicCollector(() -> {
            List<MetricSample> samples = new ArrayList<>();

            // Raft consensus state
            if (raftNode != null) {
                samples.add(new MetricSample("pulsemq_raft_current_term", raftNode.currentTerm(),
                        "Current Raft consensus term", MetricType.GAUGE));
                samples.add(new MetricSample("pulsemq_raft_is_leader",
                        (raftNode.role() == io.pulsemq.consensus.RaftRole.LEADER) ? 1.0 : 0.0,
                        "1 if this broker is the Raft leader/active controller, 0 otherwise", MetricType.GAUGE));
            }

            // Cluster topology from metadata image
            if (stateMachine != null) {
                MetadataImage image = stateMachine.currentImage();
                if (image != null) {
                    samples.add(new MetricSample("pulsemq_cluster_brokers_count", image.brokers().size(),
                            "Total number of registered live brokers in cluster", MetricType.GAUGE));
                    samples.add(new MetricSample("pulsemq_cluster_topics_count", image.topics().size(),
                            "Total number of active topics in cluster", MetricType.GAUGE));

                    int totalPartitions = 0;
                    int underReplicated = 0;
                    for (MetadataImage.TopicImage topic : image.topics().values()) {
                        totalPartitions += topic.partitions().size();
                        for (MetadataImage.PartitionImage part : topic.partitions().values()) {
                            if (part.isr().size() < part.replicas().size()) {
                                underReplicated++;
                            }
                        }
                    }

                    samples.add(new MetricSample("pulsemq_cluster_partitions_count", totalPartitions,
                            "Total number of partitions across all topics", MetricType.GAUGE));
                    samples.add(new MetricSample("pulsemq_cluster_under_replicated_partitions", underReplicated,
                            "Count of partitions where in-sync replicas is less than replication factor", MetricType.GAUGE));
                }
            }

            return samples;
        });
    }

    private void registerJvmMetrics() {
        registry.registerDynamicCollector(() -> {
            Runtime rt = Runtime.getRuntime();
            double heapUsed = rt.totalMemory() - rt.freeMemory();
            double heapMax = rt.maxMemory();
            double uptimeSec = ManagementFactory.getRuntimeMXBean().getUptime() / 1000.0;

            return List.of(
                    new MetricSample("pulsemq_jvm_heap_bytes_used", heapUsed, "JVM heap memory currently used in bytes", MetricType.GAUGE),
                    new MetricSample("pulsemq_jvm_heap_bytes_max", heapMax, "JVM maximum available heap memory in bytes", MetricType.GAUGE),
                    new MetricSample("pulsemq_jvm_uptime_seconds", uptimeSec, "Broker process uptime in seconds", MetricType.GAUGE)
            );
        });
    }
}
