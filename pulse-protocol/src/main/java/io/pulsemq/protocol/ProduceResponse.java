package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public record ProduceResponse(
        ResponseHeader header,
        Map<TopicPartition, PartitionProduceResult> responses
) implements PulseResponse {

    public record PartitionProduceResult(
            ErrorCode errorCode,
            long baseOffset,
            long logAppendTime
    ) {}

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(responses.size());
        for (Map.Entry<TopicPartition, PartitionProduceResult> entry : responses.entrySet()) {
            TopicPartition tp = entry.getKey();
            byte[] topicBytes = tp.topic().getBytes(StandardCharsets.UTF_8);
            out.writeShort(topicBytes.length);
            out.writeBytes(topicBytes);
            out.writeInt(tp.partition());

            PartitionProduceResult res = entry.getValue();
            out.writeShort(res.errorCode().code());
            out.writeLong(res.baseOffset());
            out.writeLong(res.logAppendTime());
        }
    }

    public static ProduceResponse readFrom(ResponseHeader header, ByteBuf in) {
        int count = in.readInt();
        Map<TopicPartition, PartitionProduceResult> responses = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            short topicLen = in.readShort();
            byte[] topicBytes = new byte[topicLen];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);
            int partition = in.readInt();

            short errCode = in.readShort();
            long baseOffset = in.readLong();
            long logAppendTime = in.readLong();

            responses.put(
                    new TopicPartition(topic, partition),
                    new PartitionProduceResult(ErrorCode.fromCode(errCode), baseOffset, logAppendTime)
            );
        }
        return new ProduceResponse(header, responses);
    }
}
