package io.pulsemq.consensus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RaftNodeTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.close();
        }
    }

    @Test
    void testSingleNodeBecomesLeaderAndCommitsProposals() throws Exception {
        AtomicReference<String> appliedState = new AtomicReference<>();
        RaftStateMachine stateMachine = entry -> {
            appliedState.set(new String(entry.data(), StandardCharsets.UTF_8));
        };

        node = new RaftNode(1, Collections.emptyMap(), stateMachine);

        // Allow election timer to trigger (single node should elect itself immediately)
        Thread.sleep(400);

        assertThat(node.role()).isEqualTo(RaftRole.LEADER);
        assertThat(node.currentLeaderId()).isEqualTo(1);

        CompletableFuture<Long> commitFuture = node.propose("CREATE_TOPIC:events:3".getBytes(StandardCharsets.UTF_8));
        Long committedIndex = commitFuture.get(2, TimeUnit.SECONDS);

        assertThat(committedIndex).isGreaterThanOrEqualTo(1L);
        assertThat(appliedState.get()).isEqualTo("CREATE_TOPIC:events:3");
    }
}
