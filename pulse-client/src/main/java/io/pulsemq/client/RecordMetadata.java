package io.pulsemq.client;

import io.pulsemq.common.TopicPartition;

public record RecordMetadata(
        TopicPartition topicPartition,
        long offset,
        long timestamp
) {}
