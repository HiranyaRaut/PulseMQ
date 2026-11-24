package io.pulsemq.client;

import io.pulsemq.common.Record;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A batch of records accumulating in memory destined for a specific TopicPartition.
 */
public class ProducerBatch {

    private final TopicPartition topicPartition;
    private final ByteBuffer buffer;
    private final int maxBatchSize;
    private final long createdTimestamp;
    private final RecordBatch.Builder builder;
    private final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();

    private int approximateBytes = 0;
    private volatile boolean closed = false;

    public ProducerBatch(TopicPartition topicPartition, ByteBuffer buffer, int maxBatchSize) {
        this.topicPartition = topicPartition;
        this.buffer = buffer;
        this.maxBatchSize = maxBatchSize;
        this.createdTimestamp = System.currentTimeMillis();
        this.builder = RecordBatch.builder(0L);
    }

    public synchronized boolean tryAppend(ProducerRecord record, CompletableFuture<RecordMetadata> future) {
        if (closed) return false;

        int recordSize = 16 + (record.key() == null ? 0 : record.key().length) +
                (record.value() == null ? 0 : record.value().length);

        if (approximateBytes + recordSize > maxBatchSize && !futures.isEmpty()) {
            return false;
        }

        builder.append(new Record(record.key(), record.value()));
        futures.add(future);
        approximateBytes += recordSize;
        return true;
    }

    public synchronized RecordBatch complete() {
        closed = true;
        return builder.build();
    }

    public boolean isFull() {
        return approximateBytes >= maxBatchSize;
    }

    public boolean hasLingerElapsed(long lingerMs, long now) {
        return now - createdTimestamp >= lingerMs;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public List<CompletableFuture<RecordMetadata>> futures() {
        return futures;
    }

    public int recordsCount() {
        return futures.size();
    }
}
