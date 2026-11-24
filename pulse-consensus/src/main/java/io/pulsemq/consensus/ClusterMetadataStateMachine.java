package io.pulsemq.consensus;

import io.pulsemq.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Deterministic replicated state machine that applies Raft-committed MetadataRecords
 * and maintains the active cluster MetadataImage.
 */
public class ClusterMetadataStateMachine implements RaftStateMachine {

    private static final Logger log = LoggerFactory.getLogger(ClusterMetadataStateMachine.class);

    @FunctionalInterface
    public interface MetadataListener {
        void onMetadataChanged(MetadataImage newImage);
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final List<MetadataListener> listeners = new CopyOnWriteArrayList<>();

    private final Map<Integer, MetadataImage.BrokerRegistration> brokers = new HashMap<>();
    private final Map<String, MetadataImage.TopicImage> topics = new HashMap<>();
    private volatile MetadataImage currentImage = MetadataImage.empty();

    public void addListener(MetadataListener listener) {
        listeners.add(listener);
        listener.onMetadataChanged(currentImage);
    }

    public MetadataImage currentImage() {
        return currentImage;
    }

    @Override
    public void apply(RaftLogEntry entry) {
        if (entry.data() == null || entry.data().length == 0) return;

        lock.lock();
        try {
            MetadataRecord record = MetadataRecord.deserialize(entry.data());
            applyRecord(record);

            // Publish immutable snapshot
            Map<Integer, MetadataImage.BrokerRegistration> brokersCopy = new HashMap<>(brokers);
            Map<String, MetadataImage.TopicImage> topicsCopy = new HashMap<>(topics);
            currentImage = new MetadataImage(Collections.unmodifiableMap(brokersCopy), Collections.unmodifiableMap(topicsCopy));

            for (MetadataListener listener : listeners) {
                try {
                    listener.onMetadataChanged(currentImage);
                } catch (Throwable t) {
                    log.error("Error in metadata listener notification", t);
                }
            }
        } catch (Exception e) {
            log.error("Failed to apply Raft log entry at index {}", entry.index(), e);
        } finally {
            lock.unlock();
        }
    }

    private void applyRecord(MetadataRecord record) {
        switch (record) {
            case MetadataRecord.RegisterBrokerRecord r -> {
                brokers.put(r.brokerId(), new MetadataImage.BrokerRegistration(
                        r.brokerId(), r.host(), r.port(), r.registeredTime(), r.fenced()
                ));
                log.info("Applied RegisterBrokerRecord: id={}, host={}:{}", r.brokerId(), r.host(), r.port());
            }
            case MetadataRecord.TopicRecord r -> {
                topics.putIfAbsent(r.topicName(), new MetadataImage.TopicImage(
                        r.topicName(), r.topicId(), new HashMap<>()
                ));
                log.info("Applied TopicRecord: topic={}, id={}", r.topicName(), r.topicId());
            }
            case MetadataRecord.PartitionRecord r -> {
                MetadataImage.TopicImage ti = topics.get(r.topic());
                if (ti == null) {
                    ti = new MetadataImage.TopicImage(r.topic(), UUID.randomUUID().toString(), new HashMap<>());
                    topics.put(r.topic(), ti);
                }
                TopicPartition tp = new TopicPartition(r.topic(), r.partitionId());
                ti.partitions().put(r.partitionId(), new MetadataImage.PartitionImage(
                        tp, r.leaderId(), r.leaderEpoch(), r.replicas(), r.isr()
                ));
                log.info("Applied PartitionRecord: {} leader={}, epoch={}, isr={}",
                        tp, r.leaderId(), r.leaderEpoch(), r.isr());
            }
            case MetadataRecord.PartitionChangeRecord r -> {
                MetadataImage.TopicImage ti = topics.get(r.topic());
                if (ti != null) {
                    MetadataImage.PartitionImage existing = ti.partitions().get(r.partitionId());
                    if (existing != null) {
                        TopicPartition tp = new TopicPartition(r.topic(), r.partitionId());
                        ti.partitions().put(r.partitionId(), new MetadataImage.PartitionImage(
                                tp, r.newLeaderId(), r.newLeaderEpoch(), existing.replicas(), r.newIsr()
                        ));
                        log.info("Applied PartitionChangeRecord: {} newLeader={}, newEpoch={}, newIsr={}",
                                tp, r.newLeaderId(), r.newLeaderEpoch(), r.newIsr());
                    }
                }
            }
            case MetadataRecord.RemoveTopicRecord r -> {
                topics.remove(r.topicName());
                log.info("Applied RemoveTopicRecord: {}", r.topicName());
            }
        }
    }
}
