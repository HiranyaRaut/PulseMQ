package io.pulsemq.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Key-value metadata attached to a record.
 */
public record Header(String key, byte[] value) {
    public Header {
        Objects.requireNonNull(key, "key cannot be null");
    }

    public Header(String key, String value) {
        this(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Header h)) return false;
        return Objects.equals(key, h.key) && Arrays.equals(value, h.value);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "Header[key=" + key + ", valueLen=" + (value == null ? 0 : value.length) + "]";
    }
}
