package io.pulsemq.broker;

import io.netty.channel.ChannelHandlerContext;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
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
                CompletableFuture<PulseResponse> future = processRequestAsync(request);
                future.thenAccept(response -> {
                    if (response != null && ctx.channel().isActive()) {
                        ctx.writeAndFlush(response);
                    }
                }).exceptionally(ex -> {
                    log.error("Error processing request: {}", request.header(), ex);
                    return null;
                });
            } catch (Throwable t) {
                log.error("Unhandled error submitting request: {}", request.header(), t);
            }
        });
    }

    private CompletableFuture<PulseResponse> processRequestAsync(PulseRequest request) {
        RequestHeader header = request.header();

        return switch (header.apiKey()) {
            case PRODUCE -> {
                ProduceRequest produce = (ProduceRequest) request;
                if (produce.acks() == 0) {
                    replicaManager.appendRecordsAsync(produce.acks(), produce.partitionData());
                    yield CompletableFuture.completedFuture(null);
                }
                yield replicaManager.appendRecordsAsync(produce.acks(), produce.partitionData())
                        .thenApply(results -> new ProduceResponse(new ResponseHeader(header.correlationId()), results));
            }
            case FETCH -> {
                FetchRequest fetch = (FetchRequest) request;
                var results = replicaManager.readRecords(
                        header, fetch.maxWaitMs(), fetch.minBytes(), fetch.maxBytes(), fetch.partitionData());
                yield CompletableFuture.completedFuture(new FetchResponse(new ResponseHeader(header.correlationId()), results));
            }
            case METADATA -> {
                MetadataRequest meta = (MetadataRequest) request;
                yield CompletableFuture.completedFuture(handleMetadata(meta));
            }
            case JOIN_GROUP -> {
                JoinGroupRequest join = (JoinGroupRequest) request;
                yield CompletableFuture.completedFuture(groupCoordinator.handleJoinGroup(join));
            }
            case HEARTBEAT -> {
                HeartbeatRequest hb = (HeartbeatRequest) request;
                yield CompletableFuture.completedFuture(groupCoordinator.handleHeartbeat(hb));
            }
            case OFFSET_COMMIT -> {
                OffsetCommitRequest oc = (OffsetCommitRequest) request;
                yield CompletableFuture.completedFuture(groupCoordinator.handleOffsetCommit(oc));
            }
            case OFFSET_FETCH -> {
                OffsetFetchRequest of = (OffsetFetchRequest) request;
                yield CompletableFuture.completedFuture(groupCoordinator.handleOffsetFetch(of));
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
