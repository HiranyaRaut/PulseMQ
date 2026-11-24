package io.pulsemq.storage;

import java.time.Duration;

/**
 * Configuration parameters for partition log storage.
 */
public record LogConfig(
        int maxSegmentBytes,
        int indexIntervalBytes,
        int maxIndexSizeBytes,
        long retentionMs,
        long retentionBytes,
        long flushIntervalMessages,
        long flushIntervalMs,
        CleanupPolicy cleanupPolicy,
        long deleteRetentionMs
) {
    public enum CleanupPolicy {
        DELETE,
        COMPACT,
        COMPACT_DELETE
    }

    public static final int DEFAULT_MAX_SEGMENT_BYTES = 1024 * 1024 * 1024; // 1 GB
    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096; // 4 KB
    public static final int DEFAULT_MAX_INDEX_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB preallocated
    public static final long DEFAULT_RETENTION_MS = Duration.ofDays(7).toMillis();
    public static final long DEFAULT_RETENTION_BYTES = -1L; // unlimited
    public static final long DEFAULT_FLUSH_INTERVAL_MESSAGES = 100_000L;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 1000L;
    public static final CleanupPolicy DEFAULT_CLEANUP_POLICY = CleanupPolicy.DELETE;
    public static final long DEFAULT_DELETE_RETENTION_MS = Duration.ofDays(1).toMillis();

    public static LogConfig defaultConfiguration() {
        return new LogConfig(
                DEFAULT_MAX_SEGMENT_BYTES,
                DEFAULT_INDEX_INTERVAL_BYTES,
                DEFAULT_MAX_INDEX_SIZE_BYTES,
                DEFAULT_RETENTION_MS,
                DEFAULT_RETENTION_BYTES,
                DEFAULT_FLUSH_INTERVAL_MESSAGES,
                DEFAULT_FLUSH_INTERVAL_MS,
                DEFAULT_CLEANUP_POLICY,
                DEFAULT_DELETE_RETENTION_MS
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxSegmentBytes = DEFAULT_MAX_SEGMENT_BYTES;
        private int indexIntervalBytes = DEFAULT_INDEX_INTERVAL_BYTES;
        private int maxIndexSizeBytes = DEFAULT_MAX_INDEX_SIZE_BYTES;
        private long retentionMs = DEFAULT_RETENTION_MS;
        private long retentionBytes = DEFAULT_RETENTION_BYTES;
        private long flushIntervalMessages = DEFAULT_FLUSH_INTERVAL_MESSAGES;
        private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        private CleanupPolicy cleanupPolicy = DEFAULT_CLEANUP_POLICY;
        private long deleteRetentionMs = DEFAULT_DELETE_RETENTION_MS;

        public Builder maxSegmentBytes(int maxSegmentBytes) {
            this.maxSegmentBytes = maxSegmentBytes;
            return this;
        }

        public Builder indexIntervalBytes(int indexIntervalBytes) {
            this.indexIntervalBytes = indexIntervalBytes;
            return this;
        }

        public Builder maxIndexSizeBytes(int maxIndexSizeBytes) {
            this.maxIndexSizeBytes = maxIndexSizeBytes;
            return this;
        }

        public Builder retentionMs(long retentionMs) {
            this.retentionMs = retentionMs;
            return this;
        }

        public Builder retentionBytes(long retentionBytes) {
            this.retentionBytes = retentionBytes;
            return this;
        }

        public Builder flushIntervalMessages(long flushIntervalMessages) {
            this.flushIntervalMessages = flushIntervalMessages;
            return this;
        }

        public Builder flushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }

        public Builder cleanupPolicy(CleanupPolicy cleanupPolicy) {
            this.cleanupPolicy = cleanupPolicy;
            return this;
        }

        public Builder deleteRetentionMs(long deleteRetentionMs) {
            this.deleteRetentionMs = deleteRetentionMs;
            return this;
        }

        public LogConfig build() {
            return new LogConfig(
                    maxSegmentBytes,
                    indexIntervalBytes,
                    maxIndexSizeBytes,
                    retentionMs,
                    retentionBytes,
                    flushIntervalMessages,
                    flushIntervalMs,
                    cleanupPolicy,
                    deleteRetentionMs
            );
        }
    }
}
