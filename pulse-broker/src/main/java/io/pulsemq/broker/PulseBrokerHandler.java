package io.pulsemq.broker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.pulsemq.protocol.PulseRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulseBrokerHandler extends SimpleChannelInboundHandler<PulseRequest> {

    private static final Logger log = LoggerFactory.getLogger(PulseBrokerHandler.class);
    private final PulseRequestHandler requestHandler;

    public PulseBrokerHandler(PulseRequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PulseRequest msg) {
        requestHandler.handleRequest(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel error on connection from {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
