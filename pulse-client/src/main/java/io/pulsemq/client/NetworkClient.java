package io.pulsemq.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.pulsemq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance non-blocking Netty network client for multiplexed binary communication with PulseMQ brokers.
 */
public class NetworkClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    private final String host;
    private final int port;
    private final AtomicInteger correlationIdGen = new AtomicInteger(1);
    private final Map<Integer, PendingResponse> pendingRequests = new ConcurrentHashMap<>();

    private EventLoopGroup group;
    private Channel channel;
    private volatile boolean connected = false;

    private record PendingResponse(ApiKeys apiKey, CompletableFuture<PulseResponse> future) {}

    public NetworkClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public synchronized void connect() throws InterruptedException {
        if (connected) return;

        group = new NioEventLoopGroup(1);
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("frameDecoder", new PulseFrameDecoder());
                        ch.pipeline().addLast("handler", new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                // Peek correlationId to resolve corresponding API key
                                int markIdx = msg.readerIndex();
                                int correlationId = msg.readInt();
                                msg.readerIndex(markIdx);

                                PendingResponse pending = pendingRequests.remove(correlationId);
                                if (pending != null) {
                                    try {
                                        PulseResponse response = PulseCodec.decodeResponse(msg, pending.apiKey());
                                        pending.future().complete(response);
                                    } catch (Throwable t) {
                                        pending.future().completeExceptionally(t);
                                    }
                                } else {
                                    log.warn("Received response with unrecognized correlationId: {}", correlationId);
                                }
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                log.error("Client channel error", cause);
                                ctx.close();
                            }
                        });
                    }
                });

        ChannelFuture future = b.connect(host, port).sync();
        channel = future.channel();
        connected = true;
    }

    public int nextCorrelationId() {
        return correlationIdGen.getAndIncrement();
    }

    public CompletableFuture<PulseResponse> send(PulseRequest request) {
        CompletableFuture<PulseResponse> future = new CompletableFuture<>();
        if (!connected || !channel.isActive()) {
            future.completeExceptionally(new IllegalStateException("Client is not connected to broker"));
            return future;
        }

        int correlationId = request.header().correlationId();
        pendingRequests.put(correlationId, new PendingResponse(request.header().apiKey(), future));

        ByteBuf buf = channel.alloc().buffer();
        PulseCodec.encodeRequest(request, buf);

        channel.writeAndFlush(buf).addListener(f -> {
            if (!f.isSuccess()) {
                pendingRequests.remove(correlationId);
                future.completeExceptionally(f.cause());
            }
        });

        return future;
    }

    @Override
    public synchronized void close() {
        if (!connected) return;
        connected = false;

        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }

        pendingRequests.forEach((cid, p) ->
                p.future().completeExceptionally(new IllegalStateException("Client closed")));
        pendingRequests.clear();
    }
}
