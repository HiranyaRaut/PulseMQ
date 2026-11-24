package io.pulsemq.client;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.Record;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.ApiKeys;
import io.pulsemq.protocol.ProduceRequest;
import io.pulsemq.protocol.ProduceResponse;
import io.pulsemq.protocol.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-throughput thread-safe producer client for publishing messages to PulseMQ topic partitions.
 * Supports configurable acks, memory buffer pooling, and micro-batching via RecordAccumulator.
 */
public class PulseProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PulseProducer.class);

    private final NetworkClient networkClient;
    private final String clientId;
    private final short acks;
    private final RecordAccumulator accumulator;
    private final boolean batchingEnabled;
    private final Thread senderThread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public PulseProducer(String host, int port, String clientId, short acks, int batchSize, long lingerMs)
            throws InterruptedException {
        this.clientId = clientId;
        this.acks = acks;
        this.networkClient = new NetworkClient(host, port);
        this.networkClient.connect();

        if (batchSize > 0 && lingerMs >= 0) {
            this.batchingEnabled = true;
            this.accumulator = new RecordAccumulator(batchSize, 32 * 1024 * 1024L, lingerMs);
            this.senderThread = Thread.ofVirtual().name("pulse-sender-" + clientId).start(this::runSenderLoop);
        } else {
            this.batchingEnabled = false;
            this.accumulator = null;
            this.senderThread = null;
        }
    }

    public PulseProducer(String host, int port, String clientId, short acks) throws InterruptedException {
        this(host, port, clientId, acks, 0, 0); // direct send mode by default
    }

    public PulseProducer(String host, int port) throws InterruptedException {
        this(host, port, "pulse-producer", (short) 1);
    }

    public CompletableFuture<RecordMetadata> send(ProducerRecord record) {
        int partition = record.partition() != null ? record.partition() : 0;
        TopicPartition tp = new TopicPartition(record.topic(), partition);

        if (batchingEnabled) {
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            try {
                accumulator.append(tp, record, future, Duration.ofSeconds(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            }
            return future;
        }

        // Direct send mode
        RecordBatch batch = RecordBatch.builder(0L)
                .append(new Record(record.key(), record.value()))
                .build();

        int correlationId = networkClient.nextCorrelationId();
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 1, correlationId, clientId);
        ProduceRequest request = new ProduceRequest(header, acks, 5000, Map.of(tp, batch));

        if (acks == 0) {
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

    private void runSenderLoop() {
        while (running.get() || !Thread.currentThread().isInterrupted()) {
            try {
                sendBatches(false);
                Thread.sleep(2);
            } catch (InterruptedException e) {
                break;
            } catch (Throwable t) {
                log.error("Error in producer sender loop", t);
            }
        }
    }

    private void sendBatches(boolean force) {
        if (!batchingEnabled) return;
        Map<TopicPartition, List<ProducerBatch>> ready = accumulator.drain(System.currentTimeMillis(), force);
        if (ready.isEmpty()) return;

        Map<TopicPartition, RecordBatch> payload = new HashMap<>();
        List<ProducerBatch> allBatches = new ArrayList<>();

        for (Map.Entry<TopicPartition, List<ProducerBatch>> entry : ready.entrySet()) {
            for (ProducerBatch pb : entry.getValue()) {
                payload.put(entry.getKey(), pb.complete());
                allBatches.add(pb);
            }
        }

        int correlationId = networkClient.nextCorrelationId();
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 1, correlationId, clientId);
        ProduceRequest request = new ProduceRequest(header, acks, 5000, payload);

        networkClient.send(request).whenComplete((resp, err) -> {
            try {
                if (err != null) {
                    for (ProducerBatch pb : allBatches) {
                        for (CompletableFuture<RecordMetadata> f : pb.futures()) {
                            f.completeExceptionally(err);
                        }
                    }
                    return;
                }

                ProduceResponse pResp = (ProduceResponse) resp;
                for (ProducerBatch pb : allBatches) {
                    TopicPartition tp = pb.topicPartition();
                    ProduceResponse.PartitionProduceResult result = pResp.responses().get(tp);
                    if (result != null && result.errorCode() == ErrorCode.NONE) {
                        long baseOffset = result.baseOffset();
                        long ts = result.logAppendTime();
                        for (int i = 0; i < pb.futures().size(); i++) {
                            pb.futures().get(i).complete(new RecordMetadata(tp, baseOffset + i, ts));
                        }
                    } else {
                        ErrorCode ec = result != null ? result.errorCode() : ErrorCode.UNKNOWN_SERVER_ERROR;
                        for (CompletableFuture<RecordMetadata> f : pb.futures()) {
                            f.completeExceptionally(new RuntimeException("Produce failed with error: " + ec));
                        }
                    }
                }
            } finally {
                for (ProducerBatch pb : allBatches) {
                    accumulator.deallocate(pb);
                }
            }
        });
    }

    public void flush() {
        if (batchingEnabled) {
            sendBatches(true);
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        if (batchingEnabled) {
            flush();
            if (senderThread != null) {
                senderThread.interrupt();
            }
        }
        networkClient.close();
    }
}
