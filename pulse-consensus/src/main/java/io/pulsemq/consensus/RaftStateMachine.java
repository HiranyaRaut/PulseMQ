package io.pulsemq.consensus;

@FunctionalInterface
public interface RaftStateMachine {
    void apply(RaftLogEntry entry);
}
