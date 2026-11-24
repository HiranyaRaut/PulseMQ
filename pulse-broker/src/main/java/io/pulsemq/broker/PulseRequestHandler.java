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
    private final io.pulsemq.consensus.ClusterMetadataController metadataController;
    private final ExecutorService virtualThreadExecutor;

    public PulseRequestHandler(BrokerConfig config, ReplicaManager replicaManager, GroupCoordinator groupCoordinator) {
        this(config, replicaManager, groupCoordinator, null);
    }

    public PulseRequestHandler(
            BrokerConfig config,
            ReplicaManager replicaManager,
            GroupCoordinator groupCoordinator,
            io.pulsemq.consensus.ClusterMetadataController metadataController
    ) {
        this.config = config;
        this.replicaManager = replicaManager;
        this.groupCoordinator = groupCoordinator;
        this.metadataController = metadataController;
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
            case CREATE_TOPICS -> {
                CreateTopicsRequest create = (CreateTopicsRequest) request;
                yield handleCreateTopicsAsync(header, create);
            }
            case DELETE_TOPICS -> {
                DeleteTopicsRequest delete = (DeleteTopicsRequest) request;
                yield handleDeleteTopicsAsync(header, delete);
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

    private CompletableFuture<PulseResponse> handleCreateTopicsAsync(RequestHeader header, CreateTopicsRequest request) {
        if (metadataController == null) {
            Map<String, ErrorCode> errors = new HashMap<>();
            for (String topicName : request.topics().keySet()) {
                errors.put(topicName, ErrorCode.UNKNOWN_SERVER_ERROR);
            }
            return CompletableFuture.completedFuture(new CreateTopicsResponse(new ResponseHeader(header.correlationId()), errors));
        }

        List<CompletableFuture<Map.Entry<String, ErrorCode>>> futures = new ArrayList<>();
        for (Map.Entry<String, CreateTopicsRequest.TopicDetails> entry : request.topics().entrySet()) {
            String topicName = entry.getKey();
            CreateTopicsRequest.TopicDetails detail = entry.getValue();
            CompletableFuture<Map.Entry<String, ErrorCode>> f = metadataController.createTopic(
                    topicName,
                    detail.numPartitions(),
                    detail.replicationFactor()
            ).thenApply(code -> Map.entry(topicName, code));
            futures.add(f);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    Map<String, ErrorCode> results = new HashMap<>();
                    for (var f : futures) {
                        var entry = f.join();
                        results.put(entry.getKey(), entry.getValue());
                    }
                    return new CreateTopicsResponse(new ResponseHeader(header.correlationId()), results);
                });
    }

    private CompletableFuture<PulseResponse> handleDeleteTopicsAsync(RequestHeader header, DeleteTopicsRequest request) {
        if (metadataController == null) {
            Map<String, ErrorCode> errors = new HashMap<>();
            for (String topic : request.topics()) {
                errors.put(topic, ErrorCode.UNKNOWN_SERVER_ERROR);
            }
            return CompletableFuture.completedFuture(new DeleteTopicsResponse(new ResponseHeader(header.correlationId()), errors));
        }

        List<CompletableFuture<Map.Entry<String, ErrorCode>>> futures = new ArrayList<>();
        for (String topic : request.topics()) {
            CompletableFuture<Map.Entry<String, ErrorCode>> f = metadataController.deleteTopic(topic)
                    .thenApply(code -> Map.entry(topic, code));
            futures.add(f);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    Map<String, ErrorCode> results = new HashMap<>();
                    for (var f : futures) {
                        var entry = f.join();
                        results.put(entry.getKey(), entry.getValue());
                    }
                    return new DeleteTopicsResponse(new ResponseHeader(header.correlationId()), results);
                });
    }

    private MetadataResponse handleMetadata(MetadataRequest request) {
        List<MetadataResponse.BrokerNode> brokers = new ArrayList<>();
        Map<String, List<MetadataResponse.PartitionMetadata>> topicPartitions = new HashMap<>();

        if (metadataController != null) {
            io.pulsemq.consensus.MetadataImage image = metadataController.stateMachine().currentImage();

            // Populate brokers from metadata image
            for (io.pulsemq.consensus.MetadataImage.BrokerRegistration b : image.brokers().values()) {
                brokers.add(new MetadataResponse.BrokerNode(b.brokerId(), b.host(), b.port()));
            }

            // Populate topics from metadata image
            Set<String> requestedTopics = new HashSet<>(request.topics());
            boolean all = requestedTopics.isEmpty();

            for (io.pulsemq.consensus.MetadataImage.TopicImage ti : image.topics().values()) {
                if (!all && !requestedTopics.contains(ti.name())) continue;

                List<MetadataResponse.PartitionMetadata> partList = new ArrayList<>();
                for (io.pulsemq.consensus.MetadataImage.PartitionImage pi : ti.partitions().values()) {
                    partList.add(new MetadataResponse.PartitionMetadata(
                            pi.topicPartition().partition(),
                            pi.leaderId(),
                            pi.replicas(),
                            pi.isr(),
                            ErrorCode.NONE
                    ));
                }
                topicPartitions.put(ti.name(), partList);
            }
        }

        if (brokers.isEmpty()) {
            brokers.add(new MetadataResponse.BrokerNode(config.brokerId(), config.host(), config.port()));
        }

        // Merge local partitions from replicaManager / logManager for backwards compatibility or auto-creation
        Set<TopicPartition> localPartitions = new HashSet<>(replicaManager.allPartitions().keySet());
        localPartitions.addAll(replicaManager.logManager().allLogs().keySet());

        Set<String> requestedTopics = new HashSet<>(request.topics());
        boolean all = requestedTopics.isEmpty();

        for (String topic : requestedTopics) {
            if (!topicPartitions.containsKey(topic)) {
                TopicPartition tp = new TopicPartition(topic, 0);
                replicaManager.getOrCreatePartition(tp);
                localPartitions.add(tp);
            }
        }

        for (TopicPartition tp : localPartitions) {
            if (!all && !requestedTopics.contains(tp.topic())) {
                continue;
            }
            if (topicPartitions.containsKey(tp.topic())) {
                continue; // Already populated from metadata image
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
