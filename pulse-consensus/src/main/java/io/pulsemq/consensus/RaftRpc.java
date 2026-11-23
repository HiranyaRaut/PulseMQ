package io.pulsemq.consensus;

import java.util.List;

public final class RaftRpc {

    private RaftRpc() {}

    public record RequestVoteArgs(
            long term,
            int candidateId,
            long lastLogIndex,
            long lastLogTerm
    ) {}

    public record RequestVoteReply(
            long term,
            boolean voteGranted
    ) {}

    public record AppendEntriesArgs(
            long term,
            int leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<RaftLogEntry> entries,
            long leaderCommit
    ) {}

    public record AppendEntriesReply(
            long term,
            boolean success,
            long matchIndex
    ) {}
}
