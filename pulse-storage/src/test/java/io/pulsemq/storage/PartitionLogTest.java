package io.pulsemq.storage;

import io.pulsemq.common.Record;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionLogTest {

    @TempDir
    Path tempDir;

    private PartitionLog partitionLog;
    private TopicPartition tp;

    @BeforeEach
    void setUp() throws Exception {
        tp = new TopicPartition("orders", 0);
        File partitionDir = tempDir.resolve("orders-0").toFile();
        LogConfig config = LogConfig.builder()
                .maxSegmentBytes(1024) // Small segment limit to trigger rolls
                .indexIntervalBytes(128)
                .build();
        partitionLog = new PartitionLog(tp, partitionDir, config);
    }

    @AfterEach
    void tearDown() {
        if (partitionLog != null) {
            partitionLog.close();
        }
    }

    @Test
    void testAppendAndRead() throws Exception {
        // Append 1st batch
        RecordBatch b1 = RecordBatch.builder(0L)
                .append("k1", "v1")
                .append("k2", "v2")
                .build();
        partitionLog.append(b1);

        assertThat(partitionLog.logEndOffset()).isEqualTo(2L);

        // Read from offset 0
        FileRecords records = partitionLog.read(0L, 4096, PartitionLog.IsolationLevel.READ_UNCOMMITTED);
        List<RecordBatch> batches = records.readBatches();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).records()).hasSize(2);
        assertThat(batches.get(0).records().get(0).valueAsString()).isEqualTo("v1");
        assertThat(batches.get(0).records().get(1).valueAsString()).isEqualTo("v2");
    }

    @Test
    void testSegmentRolling() throws Exception {
        long currentOffset = 0L;
        // Append batches until segment rolls
        for (int i = 0; i < 20; i++) {
            RecordBatch b = RecordBatch.builder(currentOffset)
                    .append("key-" + i, "data-payload-large-string-to-exceed-segment-limit-" + i)
                    .build();
            partitionLog.append(b);
            currentOffset = partitionLog.logEndOffset();
        }

        assertThat(partitionLog.segments().size()).isGreaterThan(1);
        assertThat(partitionLog.logEndOffset()).isEqualTo(20L);

        // Read from middle offset spanning rolled segments
        FileRecords records = partitionLog.read(15L, 4096, PartitionLog.IsolationLevel.READ_UNCOMMITTED);
        List<RecordBatch> batches = records.readBatches();
        assertThat(batches).isNotEmpty();
        assertThat(batches.get(0).baseOffset()).isLessThanOrEqualTo(15L);
        assertThat(batches.get(0).lastOffset()).isGreaterThanOrEqualTo(15L);
    }
}
