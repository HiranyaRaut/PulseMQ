package io.pulsemq.storage;

import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.pulsemq.common.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a contiguous byte slice from a log segment file on disk.
 * Capable of zero-copy transfer directly to network sockets via OS sendfile,
 * or deserialization into in-memory RecordBatch instances.
 */
public class FileRecords {

    private final FileChannel channel;
    private final long position;
    private final int size;

    public FileRecords(FileChannel channel, long position, int size) {
        this.channel = channel;
        this.position = position;
        this.size = size;
    }

    public long position() {
        return position;
    }

    public int sizeInBytes() {
        return size;
    }

    public FileChannel channel() {
        return channel;
    }

    /**
     * Performs zero-copy transfer directly from the disk file channel to a network socket channel.
     */
    public long transferTo(WritableByteChannel target, long targetPosition, long count) throws IOException {
        long bytesToTransfer = Math.min(count, size - targetPosition);
        if (bytesToTransfer <= 0) return 0L;
        return channel.transferTo(this.position + targetPosition, bytesToTransfer, target);
    }

    /**
     * Wraps this slice as a Netty FileRegion for zero-copy socket pipeline writes.
     */
    public FileRegion asFileRegion() {
        return new DefaultFileRegion(channel, position, size);
    }

    /**
     * Reads and parses this slice into in-memory RecordBatch objects.
     */
    public List<RecordBatch> readBatches() throws IOException {
        if (size <= 0) {
            return Collections.emptyList();
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        int read = 0;
        long currPos = position;
        while (read < size) {
            int n = channel.read(buffer, currPos);
            if (n <= 0) break;
            read += n;
            currPos += n;
        }
        buffer.flip();

        List<RecordBatch> batches = new ArrayList<>();
        while (buffer.hasRemaining()) {
            RecordBatch batch = RecordBatch.readFrom(buffer);
            if (batch == null) {
                break;
            }
            batches.add(batch);
        }
        return batches;
    }
}
