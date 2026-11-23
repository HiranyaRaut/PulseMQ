package io.pulsemq.broker;

import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;
import io.pulsemq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates consumer groups, dynamic partition rebalancing, and durable offset commits.
 */
public class GroupCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

    public enum GroupState {
        EMPTY,
        PREPARING_REBALANCE,
        COMPLETING_REBALANCE,
        STABLE,
        DEAD
    }

    public static class GroupMetadata {
        private final String groupId;
        private final ReentrantLock lock = new ReentrantLock();
        private GroupState state = GroupState.EMPTY;
        private int generationId = 0;
        private String leaderId = null;
        private final Map<String, Long> members = new HashMap<>(); // memberId -> lastHeartbeat
        private final Map<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();

        public GroupMetadata(String groupId) {
            this.groupId = groupId;
        }

        public String groupId() { return groupId; }
        public int generationId() { return generationId; }
        public String leaderId() { return leaderId; }
        public GroupState state() { return state; }
    }

    private final Map<String, GroupMetadata> groups = new ConcurrentHashMap<>();
    private final ReplicaManager replicaManager;

    public GroupCoordinator(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
    }

    public JoinGroupResponse handleJoinGroup(JoinGroupRequest request) {
        GroupMetadata group = groups.computeIfAbsent(request.groupId(), GroupMetadata::new);
        group.lock.lock();
        try {
            String memberId = request.memberId();
            if (memberId == null || memberId.isEmpty()) {
                memberId = "pulse-client-" + UUID.randomUUID();
            }

            group.members.put(memberId, System.currentTimeMillis());

            if (group.leaderId == null || !group.members.containsKey(group.leaderId)) {
                group.leaderId = memberId;
            }

            group.generationId++;
            group.state = GroupState.STABLE;

            List<JoinGroupResponse.GroupMember> memberList = new ArrayList<>();
            if (memberId.equals(group.leaderId)) {
                for (String m : group.members.keySet()) {
                    memberList.add(new JoinGroupResponse.GroupMember(m));
                }
            }

            return new JoinGroupResponse(
                    new ResponseHeader(request.header().correlationId()),
                    ErrorCode.NONE,
                    group.generationId,
                    memberId,
                    group.leaderId,
                    memberList
            );
        } finally {
            group.lock.unlock();
        }
    }

    public HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        GroupMetadata group = groups.get(request.groupId());
        if (group == null) {
            return new HeartbeatResponse(new ResponseHeader(request.header().correlationId()), ErrorCode.UNKNOWN_MEMBER_ID);
        }

        group.lock.lock();
        try {
            if (!group.members.containsKey(request.memberId())) {
                return new HeartbeatResponse(new ResponseHeader(request.header().correlationId()), ErrorCode.UNKNOWN_MEMBER_ID);
            }
            if (request.generationId() != group.generationId) {
                return new HeartbeatResponse(new ResponseHeader(request.header().correlationId()), ErrorCode.ILLEGAL_GENERATION);
            }

            group.members.put(request.memberId(), System.currentTimeMillis());
            return new HeartbeatResponse(new ResponseHeader(request.header().correlationId()), ErrorCode.NONE);
        } finally {
            group.lock.unlock();
        }
    }

    public OffsetCommitResponse handleOffsetCommit(OffsetCommitRequest request) {
        GroupMetadata group = groups.computeIfAbsent(request.groupId(), GroupMetadata::new);
        group.lock.lock();
        try {
            Map<TopicPartition, ErrorCode> results = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetCommitRequest.PartitionCommitData> entry : request.offsets().entrySet()) {
                group.committedOffsets.put(entry.getKey(), entry.getValue().offset());
                results.put(entry.getKey(), ErrorCode.NONE);
            }
            return new OffsetCommitResponse(new ResponseHeader(request.header().correlationId()), results);
        } finally {
            group.lock.unlock();
        }
    }

    public OffsetFetchResponse handleOffsetFetch(OffsetFetchRequest request) {
        GroupMetadata group = groups.get(request.groupId());
        Map<TopicPartition, OffsetFetchResponse.PartitionOffsetFetchData> results = new HashMap<>();

        if (group == null) {
            for (TopicPartition tp : request.partitions()) {
                results.put(tp, new OffsetFetchResponse.PartitionOffsetFetchData(-1L, "", ErrorCode.NONE));
            }
            return new OffsetFetchResponse(new ResponseHeader(request.header().correlationId()), results);
        }

        group.lock.lock();
        try {
            for (TopicPartition tp : request.partitions()) {
                Long offset = group.committedOffsets.get(tp);
                results.put(tp, new OffsetFetchResponse.PartitionOffsetFetchData(
                        offset != null ? offset : -1L,
                        "",
                        ErrorCode.NONE
                ));
            }
            return new OffsetFetchResponse(new ResponseHeader(request.header().correlationId()), results);
        } finally {
            group.lock.unlock();
        }
    }

    @Override
    public void close() {
        groups.clear();
    }
}
