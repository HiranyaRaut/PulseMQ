package io.pulsemq.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An append-only time index mapping timestamps to relative offsets.
 * Each entry is 12 bytes: [timestamp: 8B, relativeOffset: 4B].
 */
public class TimeIndex implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TimeIndex.class);
    public static final int ENTRY_SIZE = 12;

    private final File file;
    private final long baseOffset;
    private final int maxIndexSizeBytes;
    private final ReentrantLock lock = new ReentrantLock();

    private RandomAccessFile raf;
    private FileChannel channel;
    private MappedByteBuffer mmap;
    private int entries = 0;
    private long lastTimestamp = -1L;
    private volatile boolean closed = false;

    public record TimestampOffset(long timestamp, long offset) {}

    public TimeIndex(File file, long baseOffset, int maxIndexSizeBytes) throws Exception {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSizeBytes = Math.max(ENTRY_SIZE, maxIndexSizeBytes);

        boolean newlyCreated = !file.exists();
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();

        if (newlyCreated) {
            raf.setLength(this.maxIndexSizeBytes);
            this.entries = 0;
        } else {
            long length = file.length();
            if (length % ENTRY_SIZE != 0) {
                length = (length / ENTRY_SIZE) * ENTRY_SIZE;
                raf.setLength(length);
            }
            this.entries = (int) (length / ENTRY_SIZE);
        }

        long mapSize = newlyCreated ? this.maxIndexSizeBytes : Math.max(ENTRY_SIZE, (long) entries * ENTRY_SIZE);
        this.mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);

        if (!newlyCreated && entries > 0) {
            this.lastTimestamp = mmap.getLong((entries - 1) * ENTRY_SIZE);
        }
    }

    public int entries() {
        lock.lock();
        try {
            return entries;
        } finally {
            lock.unlock();
        }
    }

    public void append(long timestamp, long offset) {
        lock.lock();
        try {
            ensureOpen();
            long effectiveTimestamp = entries > 0 ? Math.max(timestamp, lastTimestamp) : timestamp;
            if ((entries + 1) * ENTRY_SIZE > mmap.limit()) {
                throw new IllegalStateException("TimeIndex file is full");
            }

            int relOffset = (int) (offset - baseOffset);
            int bytePos = entries * ENTRY_SIZE;
            mmap.putLong(bytePos, effectiveTimestamp);
            mmap.putInt(bytePos + 8, relOffset);

            entries++;
            lastTimestamp = effectiveTimestamp;
        } finally {
            lock.unlock();
        }
    }

    public TimestampOffset lookup(long targetTimestamp) {
        lock.lock();
        try {
            ensureOpen();
            if (entries == 0) {
                return new TimestampOffset(-1L, baseOffset);
            }

            int low = 0;
            int high = entries - 1;
            int bestMatchIdx = -1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midTs = mmap.getLong(mid * ENTRY_SIZE);
                if (midTs == targetTimestamp) {
                    bestMatchIdx = mid;
                    break;
                } else if (midTs < targetTimestamp) {
                    bestMatchIdx = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            if (bestMatchIdx == -1) {
                return new TimestampOffset(mmap.getLong(0), baseOffset + mmap.getInt(8));
            }

            long ts = mmap.getLong(bestMatchIdx * ENTRY_SIZE);
            int relOffset = mmap.getInt(bestMatchIdx * ENTRY_SIZE + 8);
            return new TimestampOffset(ts, baseOffset + relOffset);
        } finally {
            lock.unlock();
        }
    }

    public void trimToValidSize() {
        lock.lock();
        try {
            ensureOpen();
            flush();
            SparseOffsetIndex.unmapBuffer(mmap);
            mmap = null;

            long validLength = Math.max(ENTRY_SIZE, (long) entries * ENTRY_SIZE);
            channel.truncate(validLength);
            raf.setLength(validLength);
            mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, validLength);
        } catch (Exception e) {
            log.error("Failed to trim time index {}", file.getName(), e);
        } finally {
            lock.unlock();
        }
    }

    public void flush() {
        lock.lock();
        try {
            if (mmap != null) {
                mmap.force();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            try {
                flush();
            } catch (Exception ignored) {}
            SparseOffsetIndex.unmapBuffer(mmap);
            mmap = null;
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (raf != null) {
                raf.close();
            }
        } catch (Exception e) {
            log.error("Error closing timeindex {}", file.getName(), e);
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("TimeIndex " + file.getName() + " is closed");
        }
    }
}
