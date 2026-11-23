package io.pulsemq.client;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.Record;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.ApiKeys;
import io.pulsemq.protocol.ProduceRequest;
import io.pulsemq.protocol.ProduceResponse;
import io.pulsemq.protocol.RequestHeader;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe client for publishing messages to PulseMQ topic partitions.
 */
public class PulseProducer implements Closeable {

    private final NetworkClient networkClient;
    private final String clientId;
    private final short acks;
    private final AtomicLong offsetEstimator = new AtomicLong(0L);

    public PulseProducer(String host, int port, String clientId, short acks) throws InterruptedException {
        this.clientId = clientId;
        this.acks = acks;
        this.networkClient = new NetworkClient(host, port);
        this.networkClient.connect();
    }

    public PulseProducer(String host, int port) throws InterruptedException {
        this(host, port, "pulse-producer", (short) 1);
    }

    public CompletableFuture<RecordMetadata> send(ProducerRecord record) {
        int partition = record.partition() != null ? record.partition() : 0;
        TopicPartition tp = new TopicPartition(record.topic(), partition);

        // Fetch or advance local offset
        long baseOffset = offsetEstimator.getAndIncrement();

        RecordBatch batch = RecordBatch.builder(baseOffset)
                .append(new Record(record.key(), record.value()))
                .build();

        int correlationId = networkClient.nextCorrelationId();
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 1, correlationId, clientId);
        ProduceRequest request = new ProduceRequest(header, acks, 5000, Map.of(tp, batch));

        if (acks == 0) {
            // Unacknowledged
            networkClient.send(request);
            return CompletableFuture.completedFuture(new RecordMetadata(tp, -1L, System.currentTimeMillis()));
        }

        return networkClient.send(request).thenApply(resp -> {
            ProduceResponse pResp = (ProduceResponse) resp;
            ProduceResponse.PartitionProduceResult result = pResp.responses().get(tp);
            if (result == null) {
                throw new IllegalStateException("Broker omitted response for partition: " + tp);
            }
            if (result.errorCode() != ErrorCode.NONE) {
                throw new RuntimeException("Produce failed with error: " + result.errorCode());
            }
            return new RecordMetadata(tp, result.baseOffset(), result.logAppendTime());
        });
    }

    @Override
    public void close() {
        networkClient.close();
    }
}
