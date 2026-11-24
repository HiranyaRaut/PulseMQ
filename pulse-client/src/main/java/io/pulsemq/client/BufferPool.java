package io.pulsemq.client;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reusable memory buffer pool to eliminate heap allocations and garbage collection
 * pauses during high-throughput producer ingestion.
 */
public class BufferPool {

    private final long totalMemory;
    private final int poolableSize;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final Deque<ByteBuffer> free = new ArrayDeque<>();

    private long availableMemory;

    public BufferPool(long totalMemory, int poolableSize) {
        this.totalMemory = totalMemory;
        this.poolableSize = poolableSize;
        this.availableMemory = totalMemory;
    }

    /**
     * Allocate a buffer of the given size. If poolableSize, attempts to reuse a cached buffer.
     */
    public ByteBuffer allocate(int size, Duration maxWait) throws InterruptedException {
        if (size > totalMemory) {
            throw new IllegalArgumentException("Requested size " + size + " exceeds total pool size " + totalMemory);
        }

        long nanos = maxWait.toNanos();
        lock.lock();
        try {
            // Fast path: poolable buffer available
            if (size == poolableSize && !free.isEmpty()) {
                ByteBuffer buf = free.pollFirst();
                buf.clear();
                return buf;
            }

            // Check if sufficient total memory is available
            while (availableMemory < size) {
                if (nanos <= 0) {
                    throw new IllegalStateException("Timeout waiting for buffer memory allocation of size " + size);
                }
                nanos = condition.awaitNanos(nanos);
            }

            availableMemory -= size;
            return ByteBuffer.allocate(size);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return a buffer back to the pool.
     */
    public void deallocate(ByteBuffer buffer) {
        lock.lock();
        try {
            int size = buffer.capacity();
            if (size == poolableSize) {
                buffer.clear();
                free.addLast(buffer);
            } else {
                availableMemory += size;
            }
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    public long availableMemory() {
        lock.lock();
        try {
            return availableMemory + ((long) free.size() * poolableSize);
        } finally {
            lock.unlock();
        }
    }

    public int freeBuffersCount() {
        lock.lock();
        try {
            return free.size();
        } finally {
            lock.unlock();
        }
    }
}
