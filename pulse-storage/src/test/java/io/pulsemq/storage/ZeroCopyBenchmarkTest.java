package io.pulsemq.storage;

import io.pulsemq.common.Record;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ZeroCopyBenchmarkTest {

    @TempDir
    Path tempDir;

    private static class BlackholeChannel implements WritableByteChannel {
        private long bytesWritten = 0;
        private boolean open = true;

        @Override
        public int write(ByteBuffer src) {
            int rem = src.remaining();
            bytesWritten += rem;
            src.position(src.limit());
            return rem;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        public long bytesWritten() {
            return bytesWritten;
        }
    }

    @Test
    void testZeroCopyTransferVsHeapCopyPerformance() throws Exception {
        File logDir = tempDir.resolve("zerocopy-test").toFile();
        logDir.mkdirs();

        TopicPartition tp = new TopicPartition("zerocopy-bench", 0);
        LogConfig config = LogConfig.defaultConfiguration();
        PartitionLog log = new PartitionLog(tp, logDir, config);

        byte[] payload = new byte[1024]; // 1KB per record
        java.util.Arrays.fill(payload, (byte) 'Z');

        int batchCount = 2000;
        for (int i = 0; i < batchCount; i++) {
            RecordBatch batch = RecordBatch.builder(i)
                    .append(new Record(("k" + i).getBytes(), payload))
                    .build();
            log.append(batch);
        }

        long logSize = log.activeSegment().size();
        assertThat(logSize).isGreaterThan(2_000_000L); // ~2MB+ of data

        FileRecords fileRecords = log.read(0L, (int) logSize, PartitionLog.IsolationLevel.READ_UNCOMMITTED);
        assertThat(fileRecords.sizeInBytes()).isEqualTo((int) logSize);

        // 1. Benchmark OS sendfile zero-copy (FileChannel.transferTo directly to channel)
        BlackholeChannel zeroCopySink = new BlackholeChannel();
        long zcStart = System.nanoTime();
        long transferred = 0;
        while (transferred < fileRecords.sizeInBytes()) {
            long n = fileRecords.transferTo(zeroCopySink, transferred, fileRecords.sizeInBytes() - transferred);
            if (n <= 0) break;
            transferred += n;
        }
        long zcTimeNanos = System.nanoTime() - zcStart;
        double zcDurationMs = zcTimeNanos / 1_000_000.0;
        double zcMbPerSec = (transferred / (1024.0 * 1024.0)) / (zcTimeNanos / 1_000_000_000.0);

        assertThat(transferred).isEqualTo(fileRecords.sizeInBytes());
        assertThat(zeroCopySink.bytesWritten()).isEqualTo(fileRecords.sizeInBytes());

        // 2. Benchmark Heap-Buffered transfer (FileChannel.read into heap buffer + write)
        BlackholeChannel heapSink = new BlackholeChannel();
        long heapStart = System.nanoTime();
        ByteBuffer heapBuf = ByteBuffer.allocate(64 * 1024); // 64KB buffer
        long heapTransferred = 0;
        FileChannel fc = fileRecords.channel();
        long currPos = fileRecords.position();
        long totalToRead = fileRecords.sizeInBytes();

        while (heapTransferred < totalToRead) {
            heapBuf.clear();
            int limit = (int) Math.min(heapBuf.capacity(), totalToRead - heapTransferred);
            heapBuf.limit(limit);
            int read = fc.read(heapBuf, currPos + heapTransferred);
            if (read <= 0) break;
            heapBuf.flip();
            heapSink.write(heapBuf);
            heapTransferred += read;
        }
        long heapTimeNanos = System.nanoTime() - heapStart;
        double heapDurationMs = heapTimeNanos / 1_000_000.0;
        double heapMbPerSec = (heapTransferred / (1024.0 * 1024.0)) / (heapTimeNanos / 1_000_000_000.0);

        assertThat(heapTransferred).isEqualTo(fileRecords.sizeInBytes());
        assertThat(heapSink.bytesWritten()).isEqualTo(fileRecords.sizeInBytes());

        System.out.printf("Zero-Copy transferTo   : %.2f ms | %,.2f MB/sec (transferred %,d bytes)%n",
                zcDurationMs, zcMbPerSec, transferred);
        System.out.printf("Heap-Buffered transfer : %.2f ms | %,.2f MB/sec (transferred %,d bytes)%n",
                heapDurationMs, heapMbPerSec, heapTransferred);

        log.close();
    }
}
