package io.pulsemq.client;

import java.nio.charset.StandardCharsets;

public record ProducerRecord(
        String topic,
        Integer partition,
        byte[] key,
        byte[] value
) {
    public ProducerRecord(String topic, String key, String value) {
        this(
                topic,
                0,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8)
        );
    }

    public ProducerRecord(String topic, byte[] value) {
        this(topic, 0, null, value);
    }
}
