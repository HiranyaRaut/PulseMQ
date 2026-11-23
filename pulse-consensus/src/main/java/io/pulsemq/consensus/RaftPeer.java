package io.pulsemq.consensus;

import java.util.concurrent.CompletableFuture;

/**
 * RPC interface to communicate with a remote peer Raft node.
 */
public interface RaftPeer {
    int peerId();
    CompletableFuture<RaftRpc.RequestVoteReply> requestVote(RaftRpc.RequestVoteArgs args);
    CompletableFuture<RaftRpc.AppendEntriesReply> appendEntries(RaftRpc.AppendEntriesArgs args);
}
