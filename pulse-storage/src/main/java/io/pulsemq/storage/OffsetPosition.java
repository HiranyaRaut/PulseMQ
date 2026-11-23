package io.pulsemq.storage;

/**
 * Maps a logical record offset to its physical byte offset within a log segment file.
 */
public record OffsetPosition(long offset, int position) {
    public static final OffsetPosition ZERO = new OffsetPosition(0L, 0);
}
