package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.util.List;

/**
 * Server-side Netty codec: decodes inbound PulseRequests, encodes outbound PulseResponses.
 */
public class PulseServerCodec extends ByteToMessageCodec<PulseResponse> {

    @Override
    protected void encode(ChannelHandlerContext ctx, PulseResponse msg, ByteBuf out) {
        PulseCodec.encodeResponse(msg, out);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        PulseRequest request = PulseCodec.decodeRequest(in);
        out.add(request);
    }
}
