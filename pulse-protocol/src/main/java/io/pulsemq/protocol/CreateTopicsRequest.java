package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

public record CreateTopicsRequest(
        RequestHeader header,
        Map<String, TopicDetails> topics,
        int timeoutMs
) implements PulseRequest {

    public record TopicDetails(int numPartitions, short replicationFactor) {}

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(topics.size());
        for (Map.Entry<String, TopicDetails> entry : topics.entrySet()) {
            OffsetCommitRequest.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().numPartitions());
            out.writeShort(entry.getValue().replicationFactor());
        }
        out.writeInt(timeoutMs);
    }

    public static CreateTopicsRequest readFrom(RequestHeader header, ByteBuf in) {
        int count = in.readInt();
        Map<String, TopicDetails> topics = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            String name = OffsetCommitRequest.readString(in);
            int numPartitions = in.readInt();
            short replicationFactor = in.readShort();
            topics.put(name, new TopicDetails(numPartitions, replicationFactor));
        }
        int timeoutMs = in.readInt();
        return new CreateTopicsRequest(header, topics, timeoutMs);
    }
}
