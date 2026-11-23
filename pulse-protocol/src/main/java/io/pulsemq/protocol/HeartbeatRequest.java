package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;

public record HeartbeatRequest(
        RequestHeader header,
        String groupId,
        int generationId,
        String memberId
) implements PulseRequest {

    @Override
    public void writeBody(ByteBuf out) {
        OffsetCommitRequest.writeString(out, groupId);
        out.writeInt(generationId);
        OffsetCommitRequest.writeString(out, memberId);
    }

    public static HeartbeatRequest readFrom(RequestHeader header, ByteBuf in) {
        String groupId = OffsetCommitRequest.readString(in);
        int gen = in.readInt();
        String memberId = OffsetCommitRequest.readString(in);
        return new HeartbeatRequest(header, groupId, gen, memberId);
    }
}
