package io.pulsemq.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SparseOffsetIndexTest {

    @TempDir
    Path tempDir;

    private SparseOffsetIndex index;
    private File indexFile;

    @BeforeEach
    void setUp() throws Exception {
        indexFile = tempDir.resolve("00000000000000000000.index").toFile();
        index = new SparseOffsetIndex(indexFile, 0L, 1024 * 1024);
    }

    @AfterEach
    void tearDown() {
        if (index != null) {
            index.close();
        }
    }

    @Test
    void testAppendAndLookup() {
        index.append(0L, 0);
        index.append(100L, 4096);
        index.append(200L, 8192);
        index.append(300L, 12288);

        assertThat(index.entries()).isEqualTo(4);

        // Exact lookups
        assertThat(index.lookup(0L)).isEqualTo(new OffsetPosition(0L, 0));
        assertThat(index.lookup(100L)).isEqualTo(new OffsetPosition(100L, 4096));
        assertThat(index.lookup(200L)).isEqualTo(new OffsetPosition(200L, 8192));
        assertThat(index.lookup(300L)).isEqualTo(new OffsetPosition(300L, 12288));

        // In-between lookups (floor search)
        assertThat(index.lookup(50L)).isEqualTo(new OffsetPosition(0L, 0));
        assertThat(index.lookup(150L)).isEqualTo(new OffsetPosition(100L, 4096));
        assertThat(index.lookup(299L)).isEqualTo(new OffsetPosition(200L, 8192));
        assertThat(index.lookup(500L)).isEqualTo(new OffsetPosition(300L, 12288));
    }

    @Test
    void testTruncateTo() {
        index.append(0L, 0);
        index.append(100L, 4096);
        index.append(200L, 8192);
        index.append(300L, 12288);

        index.truncateTo(150L);
        assertThat(index.entries()).isEqualTo(2);
        assertThat(index.lastOffset()).isEqualTo(100L);
    }
}
