package io.pulsemq.broker;

import io.pulsemq.client.NetworkClient;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.*;
import io.pulsemq.storage.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages inter-broker replication threads. Follower brokers periodically fetch
 * event streams from partition leaders and append them to local partition logs.
 */
public class ReplicaFetcherManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaFetcherManager.class);

    private final BrokerConfig config;
    private final ReplicaManager replicaManager;
    private final Map<Integer, PeerBroker> peerBrokers = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread fetcherThread;

    public static class PeerBroker {
        private final int brokerId;
        private final String host;
        private final int port;
        private final NetworkClient client;

        public PeerBroker(int brokerId, String host, int port) {
            this.brokerId = brokerId;
            this.host = host;
            this.port = port;
            this.client = new NetworkClient(host, port);
        }

        public NetworkClient client() { return client; }
        public int brokerId() { return brokerId; }
        public String host() { return host; }
        public int port() { return port; }
    }

    public ReplicaFetcherManager(BrokerConfig config, ReplicaManager replicaManager) {
        this.config = config;
        this.replicaManager = replicaManager;
        this.fetcherThread = Thread.ofVirtual().name("replica-fetcher-" + config.brokerId()).start(this::runFetcherLoop);
    }

    public void registerPeer(int peerBrokerId, String host, int port) {
        if (peerBrokerId == config.brokerId()) return;

        peerBrokers.computeIfAbsent(peerBrokerId, id -> {
            PeerBroker peer = new PeerBroker(id, host, port);
            try {
                peer.client().connect();
                log.info("Broker [{}] connected to peer leader broker [{}] at {}:{}",
                        config.brokerId(), id, host, port);
            } catch (Exception e) {
                log.warn("Broker [{}] failed initial connect to peer broker [{}] at {}:{}",
                        config.brokerId(), id, host, port, e);
            }
            return peer;
        });
    }

    private void runFetcherLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                fetchFromLeaders();
                Thread.sleep(10); // replication poll delay
            } catch (InterruptedException e) {
                break;
            } catch (Throwable t) {
                log.error("Unexpected error in replica fetcher loop", t);
            }
        }
    }

    private void fetchFromLeaders() {
        // Group follower partitions by leaderId
        Map<Integer, Map<TopicPartition, FetchRequest.PartitionFetchData>> leaderPartitionMap = new HashMap<>();

        for (Map.Entry<TopicPartition, ReplicaManager.PartitionState> entry : replicaManager.allPartitions().entrySet()) {
            TopicPartition tp = entry.getKey();
            ReplicaManager.PartitionState state = entry.getValue();

            if (!state.isLeader(config.brokerId())) {
                int leaderId = state.leaderId();
                PartitionLog localLog = replicaManager.logManager().getLog(tp);
                long fetchOffset = localLog != null ? localLog.logEndOffset() : 0L;

                leaderPartitionMap
                        .computeIfAbsent(leaderId, k -> new HashMap<>())
                        .put(tp, new FetchRequest.PartitionFetchData(fetchOffset, 4 * 1024 * 1024));
            }
        }

        for (Map.Entry<Integer, Map<TopicPartition, FetchRequest.PartitionFetchData>> entry : leaderPartitionMap.entrySet()) {
            int leaderId = entry.getKey();
            Map<TopicPartition, FetchRequest.PartitionFetchData> fetchMap = entry.getValue();
            PeerBroker peer = peerBrokers.get(leaderId);
            if (peer == null) continue;

            String replicaClientId = "pulse-replica-" + config.brokerId();
            int correlationId = peer.client().nextCorrelationId();
            RequestHeader header = new RequestHeader(ApiKeys.FETCH, (short) 1, correlationId, replicaClientId);
            FetchRequest request = new FetchRequest(header, 500, 1, 4 * 1024 * 1024, fetchMap);

            try {
                PulseResponse response = peer.client().send(request).get(1, TimeUnit.SECONDS);
                FetchResponse fResp = (FetchResponse) response;

                for (Map.Entry<TopicPartition, FetchResponse.PartitionFetchResult> resEntry : fResp.responses().entrySet()) {
                    TopicPartition tp = resEntry.getKey();
                    FetchResponse.PartitionFetchResult res = resEntry.getValue();

                    if (res.errorCode() == ErrorCode.NONE && res.recordsData() != null && res.recordsData().length > 0) {
                        PartitionLog localLog = replicaManager.logManager().getOrCreateLog(tp);
                        ByteBuffer buf = ByteBuffer.wrap(res.recordsData());
                        while (buf.hasRemaining()) {
                            RecordBatch batch = RecordBatch.readFrom(buf);
                            if (batch == null) break;
                            if (batch.lastOffset() < localLog.logEndOffset()) {
                                continue; // Already replicated
                            }
                            localLog.append(batch);
                        }
                        localLog.updateHighWatermark(Math.min(res.highWatermark(), localLog.logEndOffset()));
                    }
                }
            } catch (Exception e) {
                log.debug("Fetch error from leader {}: {}", leaderId, e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        fetcherThread.interrupt();
        for (PeerBroker peer : peerBrokers.values()) {
            peer.client().close();
        }
        peerBrokers.clear();
    }
}
