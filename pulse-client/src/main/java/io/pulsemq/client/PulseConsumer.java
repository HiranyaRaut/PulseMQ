package io.pulsemq.client;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.Record;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.*;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Consumer client that polls topic partitions, parses RecordBatches, and commits offsets.
 */
public class PulseConsumer implements Closeable {

    private final NetworkClient networkClient;
    private final String groupId;
    private final String clientId;
    private final Set<TopicPartition> subscriptions = ConcurrentHashMap.newKeySet();
    private final Map<TopicPartition, Long> consumedOffsets = new ConcurrentHashMap<>();

    private String memberId = null;
    private int generationId = 0;

    public PulseConsumer(String host, int port, String groupId, String clientId) throws InterruptedException {
        this.groupId = groupId;
        this.clientId = clientId;
        this.networkClient = new NetworkClient(host, port);
        this.networkClient.connect();
    }

    public PulseConsumer(String host, int port, String groupId) throws InterruptedException {
        this(host, port, groupId, "pulse-consumer-" + UUID.randomUUID());
    }

    public void subscribe(String topic) {
        subscribe(List.of(topic));
    }

    public void subscribe(List<String> topics) {
        for (String topic : topics) {
            TopicPartition tp = new TopicPartition(topic, 0);
            subscriptions.add(tp);
            consumedOffsets.putIfAbsent(tp, 0L);
        }

        // Send JoinGroup
        try {
            int correlationId = networkClient.nextCorrelationId();
            RequestHeader header = new RequestHeader(ApiKeys.JOIN_GROUP, (short) 1, correlationId, clientId);
            JoinGroupRequest req = new JoinGroupRequest(header, groupId, memberId, "consumer", List.of("range"));
            JoinGroupResponse resp = (JoinGroupResponse) networkClient.send(req).get(5, TimeUnit.SECONDS);
            this.memberId = resp.memberId();
            this.generationId = resp.generationId();
        } catch (Exception ignored) {}
    }

    public void assign(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            subscriptions.add(tp);
            consumedOffsets.putIfAbsent(tp, 0L);
        }
    }

    public void assign(TopicPartition partition) {
        assign(List.of(partition));
    }

    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(subscriptions);
    }

    public List<ConsumerRecord> poll(Duration timeout) {
        if (subscriptions.isEmpty()) {
            return Collections.emptyList();
        }

        Map<TopicPartition, FetchRequest.PartitionFetchData> fetchMap = new HashMap<>();
        for (TopicPartition tp : subscriptions) {
            long currentOffset = consumedOffsets.getOrDefault(tp, 0L);
            fetchMap.put(tp, new FetchRequest.PartitionFetchData(currentOffset, 1024 * 1024));
        }

        int correlationId = networkClient.nextCorrelationId();
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, (short) 1, correlationId, clientId);
        FetchRequest req = new FetchRequest(header, (int) timeout.toMillis(), 1, 1024 * 1024, fetchMap);

        try {
            FetchResponse resp = (FetchResponse) networkClient.send(req).get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
            List<ConsumerRecord> records = new ArrayList<>();

            for (Map.Entry<TopicPartition, FetchResponse.PartitionFetchResult> entry : resp.responses().entrySet()) {
                TopicPartition tp = entry.getKey();
                FetchResponse.PartitionFetchResult res = entry.getValue();

                if (res.errorCode() == ErrorCode.NONE && res.recordsData() != null && res.recordsData().length > 0) {
                    ByteBuffer buf = ByteBuffer.wrap(res.recordsData());
                    while (buf.hasRemaining()) {
                        RecordBatch batch = RecordBatch.readFrom(buf);
                        if (batch == null) break;

                        for (Record r : batch.records()) {
                            long offset = r.offset();
                            if (offset >= consumedOffsets.getOrDefault(tp, 0L)) {
                                records.add(new ConsumerRecord(
                                        tp.topic(),
                                        tp.partition(),
                                        offset,
                                        r.timestamp(),
                                        r.key(),
                                        r.value()
                                ));
                                consumedOffsets.put(tp, offset + 1);
                            }
                        }
                    }
                }
            }

            return records;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public void commitSync() {
        commitSync(this.consumedOffsets);
    }

    public void commitSync(Map<TopicPartition, Long> offsets) {
        if (offsets == null || offsets.isEmpty()) return;

        Map<TopicPartition, OffsetCommitRequest.PartitionCommitData> commitMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            commitMap.put(entry.getKey(), new OffsetCommitRequest.PartitionCommitData(entry.getValue(), ""));
            consumedOffsets.put(entry.getKey(), entry.getValue());
        }

        int correlationId = networkClient.nextCorrelationId();
        RequestHeader header = new RequestHeader(ApiKeys.OFFSET_COMMIT, (short) 1, correlationId, clientId);
        OffsetCommitRequest req = new OffsetCommitRequest(header, groupId, generationId, memberId, commitMap);

        try {
            networkClient.send(req).get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    public long position(TopicPartition tp) {
        return consumedOffsets.getOrDefault(tp, 0L);
    }

    @Override
    public void close() {
        commitSync();
        networkClient.close();
    }
}
