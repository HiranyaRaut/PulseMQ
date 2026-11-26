package io.pulsemq.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An append-only sparse index file that maps relative offsets to physical file positions.
 * Each entry is 8 bytes: [relativeOffset: 4B, physicalPosition: 4B].
 */
public class SparseOffsetIndex implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SparseOffsetIndex.class);
    public static final int ENTRY_SIZE = 8;

    private static final Unsafe UNSAFE;
    private static final Method INVOKE_CLEANER;

    static {
        Unsafe unsafe = null;
        Method invokeCleaner = null;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
            invokeCleaner = unsafe.getClass().getMethod("invokeCleaner", ByteBuffer.class);
        } catch (Throwable t) {
            log.warn("Unable to obtain Unsafe.invokeCleaner for direct buffer cleanup", t);
        }
        UNSAFE = unsafe;
        INVOKE_CLEANER = invokeCleaner;
    }

    private final File file;
    private final long baseOffset;
    private final int maxIndexSizeBytes;
    private final ReentrantLock lock = new ReentrantLock();

    private RandomAccessFile raf;
    private FileChannel channel;
    private MappedByteBuffer mmap;
    private int entries = 0;
    private long lastOffset;
    private volatile boolean closed = false;

    public SparseOffsetIndex(File file, long baseOffset, int maxIndexSizeBytes) throws Exception {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSizeBytes = Math.max(ENTRY_SIZE, maxIndexSizeBytes);
        this.lastOffset = baseOffset;

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
            int lastRelOffset = mmap.getInt((entries - 1) * ENTRY_SIZE);
            this.lastOffset = baseOffset + lastRelOffset;
        }
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int entries() {
        lock.lock();
        try {
            return entries;
        } finally {
            lock.unlock();
        }
    }

    public long lastOffset() {
        lock.lock();
        try {
            return lastOffset;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends an entry (offset, position) into the index.
     */
    public void append(long offset, int position) {
        lock.lock();
        try {
            ensureOpen();
            if (entries > 0 && offset <= lastOffset) {
                // Ignore non-increasing offset append in sparse index without failing
                return;
            }
            if ((entries + 1) * ENTRY_SIZE > mmap.limit()) {
                throw new IllegalStateException("Index file is full, max capacity reached: " + mmap.limit());
            }

            int relOffset = (int) (offset - baseOffset);
            int bytePos = entries * ENTRY_SIZE;
            mmap.putInt(bytePos, relOffset);
            mmap.putInt(bytePos + 4, position);

            entries++;
            lastOffset = offset;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Performs binary search to find the largest entry with offset <= targetOffset.
     */
    public OffsetPosition lookup(long targetOffset) {
        lock.lock();
        try {
            ensureOpen();
            if (entries == 0) {
                return new OffsetPosition(baseOffset, 0);
            }

            int relTarget = (int) Math.max(0, targetOffset - baseOffset);
            int low = 0;
            int high = entries - 1;
            int bestMatchIdx = -1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int midOffset = mmap.getInt(mid * ENTRY_SIZE);
                if (midOffset == relTarget) {
                    bestMatchIdx = mid;
                    break;
                } else if (midOffset < relTarget) {
                    bestMatchIdx = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            if (bestMatchIdx == -1) {
                return new OffsetPosition(baseOffset, 0);
            }

            int foundRelOffset = mmap.getInt(bestMatchIdx * ENTRY_SIZE);
            int physicalPos = mmap.getInt(bestMatchIdx * ENTRY_SIZE + 4);
            return new OffsetPosition(baseOffset + foundRelOffset, physicalPos);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Truncates the index to the latest entry whose offset <= targetOffset.
     */
    public void truncateTo(long targetOffset) {
        lock.lock();
        try {
            ensureOpen();
            OffsetPosition pos = lookup(targetOffset);
            int idx = -1;
            int relTarget = (int) (pos.offset() - baseOffset);
            for (int i = 0; i < entries; i++) {
                if (mmap.getInt(i * ENTRY_SIZE) == relTarget) {
                    idx = i;
                    break;
                }
            }

            if (idx >= 0) {
                entries = idx + 1;
                lastOffset = pos.offset();
            } else {
                entries = 0;
                lastOffset = baseOffset;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resizes the underlying file to the actual number of entries * ENTRY_SIZE.
     */
    public void trimToValidSize() {
        lock.lock();
        try {
            ensureOpen();
            flush();
            unmapBuffer(mmap);
            mmap = null;

            long validLength = Math.max(ENTRY_SIZE, (long) entries * ENTRY_SIZE);
            channel.truncate(validLength);
            raf.setLength(validLength);
            mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, validLength);
        } catch (Exception e) {
            log.error("Failed to trim index file {}", file.getName(), e);
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
            unmapBuffer(mmap);
            mmap = null;
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (raf != null) {
                raf.close();
            }
        } catch (Exception e) {
            log.error("Error closing index {}", file.getName(), e);
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Index " + file.getName() + " is closed");
        }
    }

    public File file() {
        return file;
    }

    public static void unmapBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) return;
        try {
            if (UNSAFE != null && INVOKE_CLEANER != null) {
                INVOKE_CLEANER.invoke(UNSAFE, buffer);
            }
        } catch (Throwable t) {
            log.debug("Direct buffer unmap invocation failed", t);
        }
    }
}
