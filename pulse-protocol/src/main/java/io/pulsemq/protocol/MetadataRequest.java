package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record MetadataRequest(
        RequestHeader header,
        List<String> topics // empty = all topics
) implements PulseRequest {

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(topics.size());
        for (String topic : topics) {
            byte[] bytes = topic.getBytes(StandardCharsets.UTF_8);
            out.writeShort(bytes.length);
            out.writeBytes(bytes);
        }
    }

    public static MetadataRequest readFrom(RequestHeader header, ByteBuf in) {
        int count = in.readInt();
        List<String> topics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            short len = in.readShort();
            byte[] bytes = new byte[len];
            in.readBytes(bytes);
            topics.add(new String(bytes, StandardCharsets.UTF_8));
        }
        return new MetadataRequest(header, topics);
    }
}
