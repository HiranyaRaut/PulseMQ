package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public record ProduceRequest(
        RequestHeader header,
        short acks,
        int timeoutMs,
        Map<TopicPartition, RecordBatch> partitionData
) implements PulseRequest {

    @Override
    public void writeBody(ByteBuf out) {
        out.writeShort(acks);
        out.writeInt(timeoutMs);
        out.writeInt(partitionData.size());

        for (Map.Entry<TopicPartition, RecordBatch> entry : partitionData.entrySet()) {
            TopicPartition tp = entry.getKey();
            byte[] topicBytes = tp.topic().getBytes(StandardCharsets.UTF_8);
            out.writeShort(topicBytes.length);
            out.writeBytes(topicBytes);
            out.writeInt(tp.partition());

            ByteBuffer batchBuf = entry.getValue().toByteBuffer();
            out.writeInt(batchBuf.remaining());
            out.writeBytes(batchBuf);
        }
    }

    public static ProduceRequest readFrom(RequestHeader header, ByteBuf in) {
        short acks = in.readShort();
        int timeoutMs = in.readInt();
        int numPartitions = in.readInt();

        Map<TopicPartition, RecordBatch> partitionData = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            short topicLen = in.readShort();
            byte[] topicBytes = new byte[topicLen];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);
            int partition = in.readInt();

            int batchBytesLen = in.readInt();
            ByteBuffer batchBuffer = ByteBuffer.allocate(batchBytesLen);
            in.readBytes(batchBuffer);
            batchBuffer.flip();
            RecordBatch batch = RecordBatch.readFrom(batchBuffer);

            partitionData.put(new TopicPartition(topic, partition), batch);
        }

        return new ProduceRequest(header, acks, timeoutMs, partitionData);
    }
}
