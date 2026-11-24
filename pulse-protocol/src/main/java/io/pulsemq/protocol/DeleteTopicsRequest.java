package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public record DeleteTopicsRequest(
        RequestHeader header,
        List<String> topics,
        int timeoutMs
) implements PulseRequest {

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(topics.size());
        for (String topic : topics) {
            OffsetCommitRequest.writeString(out, topic);
        }
        out.writeInt(timeoutMs);
    }

    public static DeleteTopicsRequest readFrom(RequestHeader header, ByteBuf in) {
        int count = in.readInt();
        List<String> topics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            topics.add(OffsetCommitRequest.readString(in));
        }
        int timeoutMs = in.readInt();
        return new DeleteTopicsRequest(header, topics, timeoutMs);
    }
}
