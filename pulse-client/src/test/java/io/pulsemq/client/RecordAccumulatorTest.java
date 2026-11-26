package io.pulsemq.client;

import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class RecordAccumulatorTest {

    @Test
    void testBufferPoolAllocateAndRecycle() throws Exception {
        BufferPool pool = new BufferPool(1024, 256);
        assertThat(pool.availableMemory()).isEqualTo(1024);

        ByteBuffer buf1 = pool.allocate(256, Duration.ofSeconds(1));
        assertThat(pool.availableMemory()).isEqualTo(768);

        pool.deallocate(buf1);
        assertThat(pool.freeBuffersCount()).isEqualTo(1);
        assertThat(pool.availableMemory()).isEqualTo(1024);

        // Next allocation should reuse cached buffer
        ByteBuffer buf2 = pool.allocate(256, Duration.ofSeconds(1));
        assertThat(buf2).isSameAs(buf1);
    }

    @Test
    void testRecordAccumulatorBatchingAndDrain() throws Exception {
        RecordAccumulator accum = new RecordAccumulator(512, 1024 * 1024, 100);
        TopicPartition tp = new TopicPartition("orders", 0);

        CompletableFuture<RecordMetadata> f1 = new CompletableFuture<>();
        CompletableFuture<RecordMetadata> f2 = new CompletableFuture<>();

        accum.append(tp, new ProducerRecord("orders", "k1", "v1"), f1, Duration.ofSeconds(1));
        accum.append(tp, new ProducerRecord("orders", "k2", "v2"), f2, Duration.ofSeconds(1));

        // Before linger, not ready unless forced
        Map<TopicPartition, ProducerBatch> notReady = accum.drain(System.currentTimeMillis(), false);
        assertThat(notReady).isEmpty();

        // Forced drain
        Map<TopicPartition, ProducerBatch> ready = accum.drain(System.currentTimeMillis(), true);
        assertThat(ready).containsKey(tp);
        ProducerBatch batch = ready.get(tp);
        assertThat(batch).isNotNull();
        assertThat(batch.recordsCount()).isEqualTo(2);

        // Recycle batch
        accum.deallocate(batch);
        assertThat(accum.bufferPool().freeBuffersCount()).isEqualTo(1);
    }
}
