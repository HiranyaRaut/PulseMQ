package io.pulsemq.common;

import java.io.Serializable;
import java.util.Objects;

/**
 * Identifies a topic name and partition index.
 */
public record TopicPartition(String topic, int partition) implements Serializable, Comparable<TopicPartition> {
    public TopicPartition {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (partition < 0) {
            throw new IllegalArgumentException("partition cannot be negative: " + partition);
        }
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }

    @Override
    public int compareTo(TopicPartition o) {
        int cmp = this.topic.compareTo(o.topic);
        if (cmp != 0) return cmp;
        return Integer.compare(this.partition, o.partition);
    }
}
