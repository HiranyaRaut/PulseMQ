package io.pulsemq.consensus;

import io.pulsemq.common.TopicPartition;

import java.util.*;

/**
 * Immutable snapshot of the distributed cluster metadata at a given Raft log index.
 */
public record MetadataImage(
        Map<Integer, BrokerRegistration> brokers,
        Map<String, TopicImage> topics
) {
    public record BrokerRegistration(
            int brokerId,
            String host,
            int port,
            long registeredTime,
            boolean fenced
    ) {}

    public record PartitionImage(
            TopicPartition topicPartition,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicas,
            List<Integer> isr
    ) {}

    public record TopicImage(
            String name,
            String topicId,
            Map<Integer, PartitionImage> partitions
    ) {}

    public static MetadataImage empty() {
        return new MetadataImage(Collections.emptyMap(), Collections.emptyMap());
    }

    public TopicImage getTopic(String topic) {
        return topics.get(topic);
    }

    public PartitionImage getPartition(TopicPartition tp) {
        TopicImage ti = topics.get(tp.topic());
        if (ti == null) return null;
        return ti.partitions().get(tp.partition());
    }
}
