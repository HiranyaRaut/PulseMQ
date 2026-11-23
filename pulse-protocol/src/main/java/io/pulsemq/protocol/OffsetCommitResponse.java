package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public record OffsetCommitResponse(
        ResponseHeader header,
        Map<TopicPartition, ErrorCode> responses
) implements PulseResponse {

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(responses.size());
        for (Map.Entry<TopicPartition, ErrorCode> entry : responses.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetCommitRequest.writeString(out, tp.topic());
            out.writeInt(tp.partition());
            out.writeShort(entry.getValue().code());
        }
    }

    public static OffsetCommitResponse readFrom(ResponseHeader header, ByteBuf in) {
        int count = in.readInt();
        Map<TopicPartition, ErrorCode> responses = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            String topic = OffsetCommitRequest.readString(in);
            int partition = in.readInt();
            short errCode = in.readShort();
            responses.put(new TopicPartition(topic, partition), ErrorCode.fromCode(errCode));
        }
        return new OffsetCommitResponse(header, responses);
    }
}
