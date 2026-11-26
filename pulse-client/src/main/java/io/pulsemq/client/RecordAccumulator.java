package io.pulsemq.client;

import io.pulsemq.common.TopicPartition;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages producer-side record micro-batching, memory buffer pooling, and linger-based batch draining.
 */
public class RecordAccumulator {

    private final BufferPool bufferPool;
    private final int batchSize;
    private final long lingerMs;
    private final ConcurrentMap<TopicPartition, ArrayDeque<ProducerBatch>> batches = new ConcurrentHashMap<>();

    public RecordAccumulator(int batchSize, long totalMemory, long lingerMs) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.bufferPool = new BufferPool(totalMemory, batchSize);
    }

    public void append(TopicPartition tp, ProducerRecord record, CompletableFuture<RecordMetadata> future, Duration maxWait)
            throws InterruptedException {
        ArrayDeque<ProducerBatch> deque = batches.computeIfAbsent(tp, k -> new ArrayDeque<>());

        synchronized (deque) {
            ProducerBatch last = deque.peekLast();
            if (last != null && last.tryAppend(record, future)) {
                return;
            }

            ByteBuffer buffer = bufferPool.allocate(batchSize, maxWait);
            ProducerBatch newBatch = new ProducerBatch(tp, buffer, batchSize);
            newBatch.tryAppend(record, future);
            deque.addLast(newBatch);
        }
    }

    /**
     * Drains batches that are ready to be sent (either full, lingered out, or forced).
     * Drains at most one batch per TopicPartition so that each ProduceRequest carries at most one batch per partition.
     */
    public Map<TopicPartition, ProducerBatch> drain(long now, boolean force) {
        Map<TopicPartition, ProducerBatch> ready = new HashMap<>();

        for (Map.Entry<TopicPartition, ArrayDeque<ProducerBatch>> entry : batches.entrySet()) {
            TopicPartition tp = entry.getKey();
            ArrayDeque<ProducerBatch> deque = entry.getValue();

            synchronized (deque) {
                ProducerBatch first = deque.peekFirst();
                if (first == null) continue;

                if (force || first.isFull() || first.hasLingerElapsed(lingerMs, now)) {
                    ProducerBatch b = deque.pollFirst();
                    if (b != null) {
                        ready.put(tp, b);
                    }
                }
            }
        }

        return ready;
    }

    public boolean hasPending() {
        for (ArrayDeque<ProducerBatch> deque : batches.values()) {
            synchronized (deque) {
                if (!deque.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void deallocate(ProducerBatch batch) {
        bufferPool.deallocate(batch.buffer());
    }

    public BufferPool bufferPool() {
        return bufferPool;
    }
}
