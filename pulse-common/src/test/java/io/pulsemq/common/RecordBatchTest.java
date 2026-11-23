package io.pulsemq.common;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RecordBatchTest {

    @Test
    void testRecordBatchSerializationRoundTrip() {
        RecordBatch batch = RecordBatch.builder(100L)
                .leaderEpoch(1)
                .producer(1001L, (short) 1, 0)
                .append(new Record(100L, 1000L, "key-1".getBytes(), "value-1".getBytes(), List.of(new Header("h1", "v1"))))
                .append(new Record(101L, 1050L, "key-2".getBytes(), "value-2".getBytes()))
                .append(new Record(102L, 1100L, null, "value-3".getBytes()))
                .build();

        assertThat(batch.baseOffset()).isEqualTo(100L);
        assertThat(batch.lastOffset()).isEqualTo(102L);
        assertThat(batch.recordsCount()).isEqualTo(3);

        ByteBuffer buffer = batch.toByteBuffer();
        assertThat(buffer.remaining()).isEqualTo(batch.sizeInBytes());

        RecordBatch decoded = RecordBatch.readFrom(buffer);
        assertThat(decoded).isNotNull();
        assertThat(decoded.baseOffset()).isEqualTo(100L);
        assertThat(decoded.lastOffset()).isEqualTo(102L);
        assertThat(decoded.recordsCount()).isEqualTo(3);
        assertThat(decoded.producerId()).isEqualTo(1001L);

        Record r0 = decoded.records().get(0);
        assertThat(r0.offset()).isEqualTo(100L);
        assertThat(r0.keyAsString()).isEqualTo("key-1");
        assertThat(r0.valueAsString()).isEqualTo("value-1");
        assertThat(r0.headers()).hasSize(1);
        assertThat(r0.headers().get(0).key()).isEqualTo("h1");
        assertThat(new String(r0.headers().get(0).value())).isEqualTo("v1");

        Record r2 = decoded.records().get(2);
        assertThat(r2.offset()).isEqualTo(102L);
        assertThat(r2.key()).isNull();
        assertThat(r2.valueAsString()).isEqualTo("value-3");
    }
}
