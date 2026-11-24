package io.pulsemq.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Embedded Raft consensus node managing leader election, metadata log replication,
 * and quorum commits.
 */
public class RaftNode implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final int nodeId;
    private final Map<Integer, RaftPeer> peers;
    private final RaftStateMachine stateMachine;
    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService scheduler;
    private final Random random = new Random();

    // Raft persistent state
    private long currentTerm = 0;
    private int votedFor = -1;
    private final List<RaftLogEntry> raftLog = new ArrayList<>();

    // Raft volatile state
    private long commitIndex = 0;
    private long lastApplied = 0;
    private RaftRole role = RaftRole.FOLLOWER;
    private int currentLeaderId = -1;
    private long lastHeartbeatReceived = System.currentTimeMillis();

    // Leader-only volatile state
    private final Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Long>> pendingProposals = new ConcurrentHashMap<>();

    private ScheduledFuture<?> electionTimeoutTask;
    private ScheduledFuture<?> heartbeatTask;
    private volatile boolean running = true;

    public RaftNode(int nodeId, Map<Integer, RaftPeer> peers, RaftStateMachine stateMachine) {
        this.nodeId = nodeId;
        this.peers = new HashMap<>(peers);
        this.stateMachine = stateMachine;

        // Add sentinel entry at index 0
        this.raftLog.add(new RaftLogEntry(0, 0, new byte[0]));

        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "raft-node-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        if (peers.isEmpty()) {
            startElection();
        } else {
            startElectionTimer();
        }
    }

    public int nodeId() { return nodeId; }
    public RaftRole role() {
        lock.lock();
        try {
            return role;
        } finally {
            lock.unlock();
        }
    }
    public long currentTerm() {
        lock.lock();
        try {
            return currentTerm;
        } finally {
            lock.unlock();
        }
    }
    public int currentLeaderId() {
        lock.lock();
        try {
            return currentLeaderId;
        } finally {
            lock.unlock();
        }
    }
    public long commitIndex() {
        lock.lock();
        try {
            return commitIndex;
        } finally {
            lock.unlock();
        }
    }

    private long electionTimeoutMs = 150;

    private void resetElectionTimeout() {
        lastHeartbeatReceived = System.currentTimeMillis();
        electionTimeoutMs = 150 + random.nextInt(150);
    }

    private void startElectionTimer() {
        resetElectionTimeout();
        electionTimeoutTask = scheduler.scheduleAtFixedRate(this::checkElectionTimeout,
                50, 50, TimeUnit.MILLISECONDS);
    }

    private void checkElectionTimeout() {
        lock.lock();
        try {
            if (!running || role == RaftRole.LEADER) return;

            long elapsed = System.currentTimeMillis() - lastHeartbeatReceived;
            if (elapsed >= electionTimeoutMs) {
                startElection();
            }
        } finally {
            lock.unlock();
        }
    }

    private void startElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        currentLeaderId = -1;
        resetElectionTimeout();

        long term = currentTerm;
        long lastIndex = getLastLogIndex();
        long lastTerm = getLastLogTerm();

        log.info("Node {} starting election for term {}", nodeId, term);

        int votesNeeded = (peers.size() + 1) / 2 + 1;
        java.util.concurrent.atomic.AtomicInteger votesGranted = new java.util.concurrent.atomic.AtomicInteger(1); // Vote for self

        if (peers.isEmpty() || votesNeeded <= 1) {
            becomeLeader();
            return;
        }

        for (RaftPeer peer : peers.values()) {
            CompletableFuture<RaftRpc.RequestVoteReply> future = peer.requestVote(
                    new RaftRpc.RequestVoteArgs(term, nodeId, lastIndex, lastTerm)
            );

            future.thenAccept(reply -> {
                lock.lock();
                try {
                    if (!running || role != RaftRole.CANDIDATE || currentTerm != term) return;

                    if (reply.term() > currentTerm) {
                        becomeFollower(reply.term(), -1);
                        return;
                    }

                    if (reply.voteGranted()) {
                        if (votesGranted.incrementAndGet() >= votesNeeded && role == RaftRole.CANDIDATE) {
                            becomeLeader();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            });
        }
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        currentLeaderId = nodeId;
        log.info("Node {} became LEADER for term {}", nodeId, currentTerm);

        long lastLogIndex = getLastLogIndex();
        for (int peerId : peers.keySet()) {
            nextIndex.put(peerId, lastLogIndex + 1);
            matchIndex.put(peerId, 0L);
        }

        if (electionTimeoutTask != null) {
            electionTimeoutTask.cancel(false);
        }

        sendHeartbeats();
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 50, 50, TimeUnit.MILLISECONDS);
    }

    private void becomeFollower(long term, int leaderId) {
        log.info("Node {} became FOLLOWER for term {}, leaderId={}", nodeId, term, leaderId);
        role = RaftRole.FOLLOWER;
        currentTerm = term;
        votedFor = -1;
        currentLeaderId = leaderId;
        resetElectionTimeout();

        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        if (electionTimeoutTask == null || electionTimeoutTask.isCancelled()) {
            startElectionTimer();
        }
    }

    private void sendHeartbeats() {
        lock.lock();
        try {
            if (!running || role != RaftRole.LEADER) return;

            for (RaftPeer peer : peers.values()) {
                long prevIdx = nextIndex.getOrDefault(peer.peerId(), 1L) - 1;
                long prevTerm = getEntryTerm(prevIdx);
                List<RaftLogEntry> entries = new ArrayList<>();
                for (long i = prevIdx + 1; i <= getLastLogIndex(); i++) {
                    entries.add(raftLog.get((int) i));
                }

                RaftRpc.AppendEntriesArgs args = new RaftRpc.AppendEntriesArgs(
                        currentTerm, nodeId, prevIdx, prevTerm, entries, commitIndex
                );

                peer.appendEntries(args).thenAccept(reply -> {
                    lock.lock();
                    try {
                        if (!running || role != RaftRole.LEADER) return;

                        if (reply.term() > currentTerm) {
                            becomeFollower(reply.term(), -1);
                            return;
                        }

                        if (reply.success()) {
                            matchIndex.put(peer.peerId(), reply.matchIndex());
                            nextIndex.put(peer.peerId(), reply.matchIndex() + 1);
                            checkAndUpdateCommitIndex();
                        } else {
                            long currNext = nextIndex.getOrDefault(peer.peerId(), 1L);
                            if (currNext > 1) {
                                nextIndex.put(peer.peerId(), currNext - 1);
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                });
            }
        } finally {
            lock.unlock();
        }
    }

    private void checkAndUpdateCommitIndex() {
        if (peers.isEmpty()) {
            long medianIndex = getLastLogIndex();
            if (medianIndex > commitIndex && getEntryTerm(medianIndex) == currentTerm) {
                commitIndex = medianIndex;
                applyEntries();
            }
            return;
        }

        List<Long> matches = new ArrayList<>();
        matches.add(getLastLogIndex());
        for (RaftPeer peer : peers.values()) {
            matches.add(matchIndex.getOrDefault(peer.peerId(), 0L));
        }
        Collections.sort(matches);
        long quorumIndex = matches.get((matches.size() - 1) / 2);
        if (quorumIndex > commitIndex && getEntryTerm(quorumIndex) == currentTerm) {
            commitIndex = quorumIndex;
            applyEntries();
        }
    }

    private void applyEntries() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            RaftLogEntry entry = raftLog.get((int) lastApplied);
            if (stateMachine != null && entry.data().length > 0) {
                stateMachine.apply(entry);
            }
            CompletableFuture<Long> pending = pendingProposals.remove(lastApplied);
            if (pending != null) {
                pending.complete(lastApplied);
            }
        }
    }

    public CompletableFuture<Long> propose(byte[] data) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        lock.lock();
        try {
            if (role != RaftRole.LEADER) {
                future.completeExceptionally(new IllegalStateException("Not leader, current leader is: " + currentLeaderId));
                return future;
            }

            long newIndex = getLastLogIndex() + 1;
            RaftLogEntry entry = new RaftLogEntry(currentTerm, newIndex, data);
            raftLog.add(entry);
            pendingProposals.put(newIndex, future);

            if (peers.isEmpty()) {
                commitIndex = newIndex;
                applyEntries();
            } else {
                sendHeartbeats();
            }

            return future;
        } finally {
            lock.unlock();
        }
    }

    public RaftRpc.RequestVoteReply handleRequestVote(RaftRpc.RequestVoteArgs args) {
        lock.lock();
        try {
            if (args.term() > currentTerm) {
                becomeFollower(args.term(), -1);
            }

            boolean canVote = (votedFor == -1 || votedFor == args.candidateId());
            boolean isUpToDate = isLogUpToDate(args.lastLogTerm(), args.lastLogIndex());

            if (args.term() == currentTerm && canVote && isUpToDate) {
                votedFor = args.candidateId();
                resetElectionTimeout();
                return new RaftRpc.RequestVoteReply(currentTerm, true);
            }

            return new RaftRpc.RequestVoteReply(currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    public RaftRpc.AppendEntriesReply handleAppendEntries(RaftRpc.AppendEntriesArgs args) {
        lock.lock();
        try {
            if (args.term() < currentTerm) {
                return new RaftRpc.AppendEntriesReply(currentTerm, false, 0);
            }

            if (args.term() > currentTerm || role != RaftRole.FOLLOWER) {
                becomeFollower(args.term(), args.leaderId());
            }

            currentLeaderId = args.leaderId();
            resetElectionTimeout();

            if (args.prevLogIndex() > 0) {
                if (args.prevLogIndex() > getLastLogIndex() ||
                        getEntryTerm(args.prevLogIndex()) != args.prevLogTerm()) {
                    return new RaftRpc.AppendEntriesReply(currentTerm, false, getLastLogIndex());
                }
            }

            long insertIndex = args.prevLogIndex() + 1;
            for (RaftLogEntry newEntry : args.entries()) {
                if (insertIndex <= getLastLogIndex()) {
                    if (getEntryTerm(insertIndex) != newEntry.term()) {
                        // Conflict: truncate
                        while (raftLog.size() > insertIndex) {
                            raftLog.remove(raftLog.size() - 1);
                        }
                        raftLog.add(newEntry);
                    }
                } else {
                    raftLog.add(newEntry);
                }
                insertIndex++;
            }

            if (args.leaderCommit() > commitIndex) {
                commitIndex = Math.min(args.leaderCommit(), getLastLogIndex());
                applyEntries();
            }

            return new RaftRpc.AppendEntriesReply(currentTerm, true, getLastLogIndex());
        } finally {
            lock.unlock();
        }
    }

    private boolean isLogUpToDate(long lastTerm, long lastIndex) {
        long myLastTerm = getLastLogTerm();
        long myLastIndex = getLastLogIndex();
        if (lastTerm != myLastTerm) {
            return lastTerm > myLastTerm;
        }
        return lastIndex >= myLastIndex;
    }

    private long getLastLogIndex() {
        return raftLog.get(raftLog.size() - 1).index();
    }

    private long getLastLogTerm() {
        return raftLog.get(raftLog.size() - 1).term();
    }

    private long getEntryTerm(long index) {
        if (index <= 0 || index >= raftLog.size()) return 0;
        return raftLog.get((int) index).term();
    }

    public void addPeer(int peerId, RaftPeer peer) {
        lock.lock();
        try {
            peers.put(peerId, peer);
            if (role == RaftRole.LEADER) {
                nextIndex.put(peerId, getLastLogIndex() + 1);
                matchIndex.put(peerId, 0L);
            }
        } finally {
            lock.unlock();
        }
    }

    public void removePeer(int peerId) {
        lock.lock();
        try {
            peers.remove(peerId);
            nextIndex.remove(peerId);
            matchIndex.remove(peerId);
        } finally {
            lock.unlock();
        }
    }

    public Map<Integer, RaftPeer> peers() {
        lock.lock();
        try {
            return Collections.unmodifiableMap(new HashMap<>(peers));
        } finally {
            lock.unlock();
        }
    }

    public RaftStateMachine stateMachine() {
        return stateMachine;
    }

    public void becomeLeaderForTest() {
        lock.lock();
        try {
            becomeLeader();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            running = false;
            scheduler.shutdownNow();
        } finally {
            lock.unlock();
        }
    }
}
