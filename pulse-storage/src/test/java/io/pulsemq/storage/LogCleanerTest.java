package io.pulsemq.storage;

import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LogCleanerTest {

    @TempDir
    Path tempDir;

    private PartitionLog log;
    private LogCleaner cleaner;

    @BeforeEach
    void setUp() throws Exception {
        TopicPartition tp = new TopicPartition("state-updates", 0);
        File dir = tempDir.resolve("state-updates-0").toFile();
        LogConfig config = LogConfig.builder()
                .cleanupPolicy(LogConfig.CleanupPolicy.COMPACT)
                .maxSegmentBytes(256) // small segment to trigger rolling
                .indexIntervalBytes(64)
                .build();
        this.log = new PartitionLog(tp, dir, config);
        this.cleaner = new LogCleaner();
    }

    @AfterEach
    void tearDown() {
        if (log != null) {
            log.close();
        }
    }

    @Test
    void testCompactionPrunesSupersededKeys() throws Exception {
        // Append initial versions of keys
        log.append(RecordBatch.builder(0L)
                .append("user-1", "v1")
                .append("user-2", "v1")
                .build());

        // Roll segment
        log.roll(log.logEndOffset());

        // Update user-1 and add user-3
        log.append(RecordBatch.builder(log.logEndOffset())
                .append("user-1", "v2")
                .append("user-3", "v1")
                .build());

        // Roll segment
        log.roll(log.logEndOffset());

        // Update user-1 again
        log.append(RecordBatch.builder(log.logEndOffset())
                .append("user-1", "v3")
                .append("user-2", "v2")
                .build());

        // Active segment rolled so previous segments become candidates
        log.roll(log.logEndOffset());

        int pruned = cleaner.clean(log);
        assertThat(pruned).isGreaterThan(0);

        // Read all remaining records across the compacted log
        FileRecords fileRecords = log.read(0L, 8192, PartitionLog.IsolationLevel.READ_UNCOMMITTED);
        List<RecordBatch> batches = fileRecords.readBatches();

        Map<String, String> latestValues = new HashMap<>();
        for (RecordBatch batch : batches) {
            for (var r : batch.records()) {
                latestValues.put(r.keyAsString(), r.valueAsString());
            }
        }

        // Must retain latest values: user-1 -> v3, user-2 -> v2, user-3 -> v1
        assertThat(latestValues.get("user-1")).isEqualTo("v3");
        assertThat(latestValues.get("user-2")).isEqualTo("v2");
        assertThat(latestValues.get("user-3")).isEqualTo("v1");
    }
}
