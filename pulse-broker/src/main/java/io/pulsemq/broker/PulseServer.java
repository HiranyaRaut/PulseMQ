package io.pulsemq.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.pulsemq.consensus.ClusterMetadataController;
import io.pulsemq.consensus.ClusterMetadataStateMachine;
import io.pulsemq.consensus.MetadataImage;
import io.pulsemq.consensus.RaftNode;
import io.pulsemq.consensus.RaftPeer;
import io.pulsemq.protocol.PulseFrameDecoder;
import io.pulsemq.protocol.PulseServerCodec;
import io.pulsemq.storage.LogConfig;
import io.pulsemq.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

/**
 * PulseMQ Broker TCP Server powered by Netty and Java 21 Virtual Threads.
 */
public class PulseServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PulseServer.class);

    private final BrokerConfig config;
    private final LogManager logManager;
    private final ReplicaManager replicaManager;
    private final ReplicaFetcherManager replicaFetcherManager;
    private final GroupCoordinator groupCoordinator;
    private final ClusterMetadataStateMachine stateMachine;
    private final RaftNode raftNode;
    private final ClusterMetadataController metadataController;
    private final PulseRequestHandler requestHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running = false;
    private int boundPort;

    public PulseServer(BrokerConfig config) throws Exception {
        this(config, Collections.emptyMap());
    }

    public PulseServer(BrokerConfig config, Map<Integer, RaftPeer> raftPeers) throws Exception {
        this.config = config;
        this.logManager = new LogManager(config.logDir(), LogConfig.defaultConfiguration());
        this.replicaManager = new ReplicaManager(config, logManager);
        this.replicaFetcherManager = new ReplicaFetcherManager(config, replicaManager);
        this.groupCoordinator = new GroupCoordinator(replicaManager);
        this.stateMachine = new ClusterMetadataStateMachine();
        this.raftNode = new RaftNode(config.brokerId(), raftPeers, stateMachine);
        this.metadataController = new ClusterMetadataController(raftNode, stateMachine);
        this.stateMachine.addListener(this::onMetadataChanged);
        this.requestHandler = new PulseRequestHandler(config, replicaManager, groupCoordinator, metadataController);
    }

    private void onMetadataChanged(MetadataImage newImage) {
        // Register known peers in replicaFetcherManager
        for (MetadataImage.BrokerRegistration b : newImage.brokers().values()) {
            if (b.brokerId() != config.brokerId()) {
                replicaFetcherManager.registerPeer(b.brokerId(), b.host(), b.port());
            }
        }

        // Apply partition leadership/follower role assignments
        for (MetadataImage.TopicImage topic : newImage.topics().values()) {
            for (MetadataImage.PartitionImage part : topic.partitions().values()) {
                if (part.replicas().contains(config.brokerId())) {
                    replicaManager.becomeLeaderOrFollower(
                            part.topicPartition(),
                            part.leaderId(),
                            part.leaderEpoch(),
                            part.replicas()
                    );
                }
            }
        }
    }

    public synchronized void start() throws InterruptedException {
        if (running) return;

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("frameDecoder", new PulseFrameDecoder());
                        ch.pipeline().addLast("codec", new PulseServerCodec());
                        ch.pipeline().addLast("handler", new PulseBrokerHandler(requestHandler));
                    }
                });

        ChannelFuture future = b.bind(config.host(), config.port()).sync();
        serverChannel = future.channel();
        InetSocketAddress localAddr = (InetSocketAddress) serverChannel.localAddress();
        boundPort = localAddr.getPort();
        running = true;

        if (metadataController.isActiveController()) {
            try {
                metadataController.registerBroker(config.brokerId(), config.host(), boundPort);
            } catch (Exception ignored) {}
        } else {
            metadataController.recordHeartbeat(config.brokerId());
        }

        log.info("PulseMQ Broker [{}] successfully started on {}:{}",
                config.brokerId(), config.host(), boundPort);
    }

    public int boundPort() {
        return boundPort;
    }

    public boolean isRunning() {
        return running;
    }

    public BrokerConfig config() {
        return config;
    }

    public ReplicaManager replicaManager() {
        return replicaManager;
    }

    public ReplicaFetcherManager replicaFetcherManager() {
        return replicaFetcherManager;
    }

    public LogManager logManager() {
        return logManager;
    }

    public ClusterMetadataController metadataController() {
        return metadataController;
    }

    public RaftNode raftNode() {
        return raftNode;
    }

    public ClusterMetadataStateMachine stateMachine() {
        return stateMachine;
    }

    @Override
    public synchronized void close() {
        if (!running) return;
        running = false;

        log.info("Stopping PulseMQ Broker [{}]...", config.brokerId());

        if (serverChannel != null) {
            try {
                serverChannel.close().sync();
            } catch (Exception ignored) {}
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        metadataController.close();
        raftNode.close();
        requestHandler.close();
        replicaFetcherManager.close();
        groupCoordinator.close();
        replicaManager.close();
        logManager.close();

        log.info("PulseMQ Broker [{}] stopped cleanly.", config.brokerId());
    }
}
