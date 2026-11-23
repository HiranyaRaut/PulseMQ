package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Standard PulseMQ response frame header:
 * [correlationId: 4B]
 */
public record ResponseHeader(int correlationId) {
    public void writeTo(ByteBuf out) {
        out.writeInt(correlationId);
    }

    public static ResponseHeader readFrom(ByteBuf in) {
        return new ResponseHeader(in.readInt());
    }
}
