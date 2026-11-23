package io.pulsemq.storage;

import io.pulsemq.common.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A LogSegment represents an individual segment of a partitioned log.
 * It contains a data file (.log), a sparse offset index (.index), and a time index (.timeindex).
 */
public class LogSegment implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LogSegment.class);

    private final long baseOffset;
    private final File logFile;
    private final LogConfig config;
    private final SparseOffsetIndex offsetIndex;
    private final TimeIndex timeIndex;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private RandomAccessFile raf;
    private FileChannel channel;
    private volatile long size = 0L;
    private volatile long lastOffset = -1L;
    private volatile long maxTimestamp = -1L;
    private int bytesSinceLastIndexEntry = 0;
    private volatile boolean closed = false;

    public LogSegment(File dir, long baseOffset, LogConfig config) throws Exception {
        this.baseOffset = baseOffset;
        this.config = config;

        String prefix = String.format("%020d", baseOffset);
        this.logFile = new File(dir, prefix + ".log");
        File indexFile = new File(dir, prefix + ".index");
        File timeIndexFile = new File(dir, prefix + ".timeindex");

        this.offsetIndex = new SparseOffsetIndex(indexFile, baseOffset, config.maxIndexSizeBytes());
        this.timeIndex = new TimeIndex(timeIndexFile, baseOffset, config.maxIndexSizeBytes());

        this.raf = new RandomAccessFile(logFile, "rw");
        this.channel = raf.getChannel();
        this.size = channel.size();

        recoverAndVerify();
    }

    private void recoverAndVerify() throws IOException {
        if (size == 0) {
            lastOffset = baseOffset - 1;
            return;
        }

        // Scan from beginning or last known valid position to verify integrity
        ByteBuffer headerBuf = ByteBuffer.allocate(RecordBatch.RECORD_BATCH_OVERHEAD);
        long currentPos = 0L;
        long validPos = 0L;

        while (currentPos + RecordBatch.RECORD_BATCH_OVERHEAD <= size) {
            headerBuf.clear();
            channel.read(headerBuf, currentPos);
            headerBuf.flip();

            long batchBaseOffset = headerBuf.getLong();
            int batchLength = headerBuf.getInt();

            if (batchLength <= 0 || currentPos + RecordBatch.RECORD_BATCH_OVERHEAD + batchLength > size) {
                log.warn("Corrupt batch at position {}, truncating segment {} to {}", currentPos, logFile.getName(), validPos);
                break;
            }

            ByteBuffer fullBatch = ByteBuffer.allocate(RecordBatch.RECORD_BATCH_OVERHEAD + batchLength);
            channel.read(fullBatch, currentPos);
            fullBatch.flip();

            try {
                RecordBatch batch = RecordBatch.readFrom(fullBatch);
                if (batch != null) {
                    lastOffset = batch.lastOffset();
                    maxTimestamp = Math.max(maxTimestamp, batch.maxTimestamp());
                    validPos = currentPos + RecordBatch.RECORD_BATCH_OVERHEAD + batchLength;
                }
            } catch (Exception e) {
                log.warn("CRC or decoding error at position {}, truncating to {}", currentPos, validPos, e);
                break;
            }

            currentPos = validPos;
        }

        if (validPos < size) {
            channel.truncate(validPos);
            raf.setLength(validPos);
            this.size = validPos;
            offsetIndex.truncateTo(lastOffset);
        }
    }

    public long baseOffset() { return baseOffset; }
    public long size() { return size; }
    public long lastOffset() { return lastOffset; }
    public long maxTimestamp() { return maxTimestamp; }
    public File logFile() { return logFile; }
    public SparseOffsetIndex offsetIndex() { return offsetIndex; }
    public TimeIndex timeIndex() { return timeIndex; }

    /**
     * Appends a record batch to this segment and updates the sparse indexes.
     */
    public void append(RecordBatch batch) throws IOException {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            if (batch.baseOffset() <= lastOffset) {
                throw new IllegalArgumentException(
                        "Batch baseOffset " + batch.baseOffset() + " <= segment lastOffset " + lastOffset);
            }

            long physicalPos = size;
            ByteBuffer buffer = batch.toByteBuffer();
            int bytesToWrite = buffer.remaining();

            while (buffer.hasRemaining()) {
                channel.write(buffer, physicalPos + (bytesToWrite - buffer.remaining()));
            }

            // Update sparse offset index
            if (bytesSinceLastIndexEntry >= config.indexIntervalBytes() || size == 0) {
                offsetIndex.append(batch.baseOffset(), (int) physicalPos);
                timeIndex.append(batch.maxTimestamp(), batch.baseOffset());
                bytesSinceLastIndexEntry = 0;
            } else {
                bytesSinceLastIndexEntry += bytesToWrite;
            }

            size += bytesToWrite;
            lastOffset = batch.lastOffset();
            maxTimestamp = Math.max(maxTimestamp, batch.maxTimestamp());
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Reads a slice of records beginning at or after the given startOffset up to maxBytes.
     */
    public FileRecords read(long startOffset, int maxBytes) throws IOException {
        rwLock.readLock().lock();
        try {
            ensureOpen();
            if (size == 0 || startOffset > lastOffset) {
                return new FileRecords(channel, 0, 0);
            }

            // 1. Binary search offset index for nearest lower/equal position
            OffsetPosition pos = offsetIndex.lookup(startOffset);
            long searchPos = pos.position();

            // 2. Scan forward from searchPos to find exact batch containing startOffset
            ByteBuffer headerBuf = ByteBuffer.allocate(RecordBatch.RECORD_BATCH_OVERHEAD);
            long exactPos = -1L;

            while (searchPos + RecordBatch.RECORD_BATCH_OVERHEAD <= size) {
                headerBuf.clear();
                channel.read(headerBuf, searchPos);
                headerBuf.flip();

                long batchBase = headerBuf.getLong();
                int batchLen = headerBuf.getInt();

                if (batchLen <= 0 || searchPos + RecordBatch.RECORD_BATCH_OVERHEAD + batchLen > size) {
                    break;
                }

                // If batch base is already greater than startOffset or batch last offset >= startOffset
                // We inspect last offset delta
                ByteBuffer batchSummary = ByteBuffer.allocate(11);
                channel.read(batchSummary, searchPos + RecordBatch.RECORD_BATCH_OVERHEAD);
                batchSummary.flip();
                // skip epoch (4) + magic (1) + crc (4)
                batchSummary.position(9);
                short attrs = batchSummary.getShort();

                ByteBuffer deltaBuf = ByteBuffer.allocate(4);
                channel.read(deltaBuf, searchPos + RecordBatch.RECORD_BATCH_OVERHEAD + 11);
                deltaBuf.flip();
                int lastDelta = deltaBuf.getInt();
                long batchLastOffset = batchBase + lastDelta;

                if (batchLastOffset >= startOffset) {
                    exactPos = searchPos;
                    break;
                }

                searchPos += RecordBatch.RECORD_BATCH_OVERHEAD + batchLen;
            }

            if (exactPos == -1L || exactPos >= size) {
                return new FileRecords(channel, 0, 0);
            }

            int sliceBytes = (int) Math.min(maxBytes, size - exactPos);
            return new FileRecords(channel, exactPos, sliceBytes);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Truncates the segment to the given targetOffset.
     */
    public void truncateTo(long targetOffset) throws IOException {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            if (targetOffset > lastOffset) {
                return;
            }

            OffsetPosition pos = offsetIndex.lookup(targetOffset);
            long searchPos = pos.position();
            long newSize = 0L;
            long newLastOffset = baseOffset - 1;

            ByteBuffer header = ByteBuffer.allocate(RecordBatch.RECORD_BATCH_OVERHEAD);
            while (searchPos + RecordBatch.RECORD_BATCH_OVERHEAD <= size) {
                header.clear();
                channel.read(header, searchPos);
                header.flip();

                long batchBase = header.getLong();
                int batchLen = header.getInt();

                if (batchBase > targetOffset) {
                    break;
                }

                newSize = searchPos + RecordBatch.RECORD_BATCH_OVERHEAD + batchLen;
                newLastOffset = batchBase;
                searchPos = newSize;
            }

            channel.truncate(newSize);
            raf.setLength(newSize);
            this.size = newSize;
            this.lastOffset = newLastOffset;

            offsetIndex.truncateTo(targetOffset);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void flush() throws IOException {
        rwLock.writeLock().lock();
        try {
            if (closed) return;
            channel.force(true);
            offsetIndex.flush();
            timeIndex.flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        rwLock.writeLock().lock();
        try {
            if (closed) return;
            closed = true;
            try {
                flush();
            } catch (Exception ignored) {}

            offsetIndex.close();
            timeIndex.close();

            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (raf != null) {
                raf.close();
            }
        } catch (Exception e) {
            log.error("Error closing segment {}", logFile.getName(), e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Deletes underlying physical files from disk.
     */
    public void delete() {
        close();
        if (logFile.exists()) logFile.delete();
        if (offsetIndex.file().exists()) offsetIndex.file().delete();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("LogSegment " + logFile.getName() + " is closed");
        }
    }
}
