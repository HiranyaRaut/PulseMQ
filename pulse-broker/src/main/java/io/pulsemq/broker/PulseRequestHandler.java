package io.pulsemq.broker;

import io.netty.channel.ChannelHandlerContext;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Dispatches incoming PulseMQ binary requests using Java 21 Virtual Threads.
 */
public class PulseRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(PulseRequestHandler.class);

    private final BrokerConfig config;
    private final ReplicaManager replicaManager;
    private final GroupCoordinator groupCoordinator;
    private final ExecutorService virtualThreadExecutor;

    public PulseRequestHandler(BrokerConfig config, ReplicaManager replicaManager, GroupCoordinator groupCoordinator) {
        this.config = config;
        this.replicaManager = replicaManager;
        this.groupCoordinator = groupCoordinator;
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void handleRequest(ChannelHandlerContext ctx, PulseRequest request) {
        // Dispatch to lightweight Java 21 virtual thread
        virtualThreadExecutor.submit(() -> {
            try {
                PulseResponse response = processRequest(request);
                if (response != null) {
                    ctx.writeAndFlush(response);
                }
            } catch (Throwable t) {
                log.error("Unhandled error processing request: {}", request.header(), t);
            }
        });
    }

    private PulseResponse processRequest(PulseRequest request) {
        RequestHeader header = request.header();

        return switch (header.apiKey()) {
            case PRODUCE -> {
                ProduceRequest produce = (ProduceRequest) request;
                var results = replicaManager.appendRecords(produce.acks(), produce.partitionData());
                if (produce.acks() == 0) {
                    // acks=0 does not expect a response
                    yield null;
                }
                yield new ProduceResponse(new ResponseHeader(header.correlationId()), results);
            }
            case FETCH -> {
                FetchRequest fetch = (FetchRequest) request;
                var results = replicaManager.readRecords(
                        fetch.maxWaitMs(), fetch.minBytes(), fetch.maxBytes(), fetch.partitionData());
                yield new FetchResponse(new ResponseHeader(header.correlationId()), results);
            }
            case METADATA -> {
                MetadataRequest meta = (MetadataRequest) request;
                yield handleMetadata(meta);
            }
            case JOIN_GROUP -> {
                JoinGroupRequest join = (JoinGroupRequest) request;
                yield groupCoordinator.handleJoinGroup(join);
            }
            case HEARTBEAT -> {
                HeartbeatRequest hb = (HeartbeatRequest) request;
                yield groupCoordinator.handleHeartbeat(hb);
            }
            case OFFSET_COMMIT -> {
                OffsetCommitRequest oc = (OffsetCommitRequest) request;
                yield groupCoordinator.handleOffsetCommit(oc);
            }
            case OFFSET_FETCH -> {
                OffsetFetchRequest of = (OffsetFetchRequest) request;
                yield groupCoordinator.handleOffsetFetch(of);
            }
            default -> throw new UnsupportedOperationException("Unsupported ApiKey: " + header.apiKey());
        };
    }

    private MetadataResponse handleMetadata(MetadataRequest request) {
        List<MetadataResponse.BrokerNode> brokers = List.of(
                new MetadataResponse.BrokerNode(config.brokerId(), config.host(), config.port())
        );

        Map<String, List<MetadataResponse.PartitionMetadata>> topicPartitions = new HashMap<>();

        // Collect existing partitions from log manager and replica manager
        Set<TopicPartition> allPartitions = new HashSet<>(replicaManager.allPartitions().keySet());
        allPartitions.addAll(replicaManager.logManager().allLogs().keySet());

        Set<String> requestedTopics = new HashSet<>(request.topics());
        boolean all = requestedTopics.isEmpty();

        // If requested topics don't exist yet, auto-create them if default partitions configured
        for (String topic : requestedTopics) {
            TopicPartition tp = new TopicPartition(topic, 0);
            replicaManager.getOrCreatePartition(tp);
            allPartitions.add(tp);
        }

        for (TopicPartition tp : allPartitions) {
            if (!all && !requestedTopics.contains(tp.topic())) {
                continue;
            }

            var partitionState = replicaManager.getOrCreatePartition(tp);
            MetadataResponse.PartitionMetadata pm = new MetadataResponse.PartitionMetadata(
                    tp.partition(),
                    partitionState.leaderId(),
                    partitionState.replicas(),
                    partitionState.isr(),
                    ErrorCode.NONE
            );

            topicPartitions.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(pm);
        }

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        for (Map.Entry<String, List<MetadataResponse.PartitionMetadata>> entry : topicPartitions.entrySet()) {
            topicMetadata.add(new MetadataResponse.TopicMetadata(
                    entry.getKey(),
                    ErrorCode.NONE,
                    entry.getValue()
            ));
        }

        return new MetadataResponse(new ResponseHeader(request.header().correlationId()), brokers, topicMetadata);
    }

    public void close() {
        virtualThreadExecutor.shutdown();
    }
}
