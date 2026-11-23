package io.pulsemq.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * An individual log record within a batch.
 */
public record Record(
        long offset,
        long timestamp,
        byte[] key,
        byte[] value,
        List<Header> headers
) {
    public Record(long offset, long timestamp, byte[] key, byte[] value) {
        this(offset, timestamp, key, value, List.of());
    }

    public Record(byte[] key, byte[] value) {
        this(-1L, System.currentTimeMillis(), key, value, List.of());
    }

    public Record(String key, String value) {
        this(
                -1L,
                System.currentTimeMillis(),
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8),
                List.of()
        );
    }

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public Record withOffset(long newOffset) {
        return new Record(newOffset, timestamp, key, value, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record r)) return false;
        return offset == r.offset &&
                timestamp == r.timestamp &&
                Arrays.equals(key, r.key) &&
                Arrays.equals(value, r.value) &&
                headers.equals(r.headers);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + headers.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "offset=" + offset +
                ", ts=" + timestamp +
                ", key=" + (key == null ? "null" : keyAsString()) +
                ", valLen=" + (value == null ? 0 : value.length) +
                '}';
    }
}
