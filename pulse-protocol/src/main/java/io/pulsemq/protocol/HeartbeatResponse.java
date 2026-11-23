package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;

public record HeartbeatResponse(
        ResponseHeader header,
        ErrorCode errorCode
) implements PulseResponse {

    @Override
    public void writeBody(ByteBuf out) {
        out.writeShort(errorCode.code());
    }

    public static HeartbeatResponse readFrom(ResponseHeader header, ByteBuf in) {
        short code = in.readShort();
        return new HeartbeatResponse(header, ErrorCode.fromCode(code));
    }
}
