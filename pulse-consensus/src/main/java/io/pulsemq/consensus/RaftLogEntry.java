package io.pulsemq.consensus;

import java.util.Arrays;
import java.util.Objects;

/**
 * A replicated Raft log entry.
 */
public record RaftLogEntry(
        long term,
        long index,
        byte[] data
) {
    public RaftLogEntry {
        Objects.requireNonNull(data, "data cannot be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaftLogEntry that)) return false;
        return term == that.term && index == that.index && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, index);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
