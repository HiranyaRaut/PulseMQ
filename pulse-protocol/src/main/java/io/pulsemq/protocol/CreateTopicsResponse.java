package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;

import java.util.HashMap;
import java.util.Map;

public record CreateTopicsResponse(
        ResponseHeader header,
        Map<String, ErrorCode> results
) implements PulseResponse {

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(results.size());
        for (Map.Entry<String, ErrorCode> entry : results.entrySet()) {
            OffsetCommitRequest.writeString(out, entry.getKey());
            out.writeShort(entry.getValue().code());
        }
    }

    public static CreateTopicsResponse readFrom(ResponseHeader header, ByteBuf in) {
        int count = in.readInt();
        Map<String, ErrorCode> results = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            String topic = OffsetCommitRequest.readString(in);
            short errCode = in.readShort();
            results.put(topic, ErrorCode.fromCode(errCode));
        }
        return new CreateTopicsResponse(header, results);
    }
}
