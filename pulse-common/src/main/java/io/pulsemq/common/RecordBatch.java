package io.pulsemq.common;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A batch of records stored sequentially on disk or sent across the network.
 * Layout:
 * [baseOffset: 8B]
 * [batchLength: 4B] (length from partitionLeaderEpoch to end of batch)
 * [partitionLeaderEpoch: 4B]
 * [magic: 1B]
 * [crc: 4B] (CRC32C of everything from attributes to end)
 * [attributes: 2B] (bits 0-2: compression, bit 3: timestamp type)
 * [lastOffsetDelta: 4B]
 * [firstTimestamp: 8B]
 * [maxTimestamp: 8B]
 * [producerId: 8B]
 * [producerEpoch: 2B]
 * [baseSequence: 4B]
 * [recordsCount: 4B]
 * [Records...]
 */
public class RecordBatch {

    public static final byte CURRENT_MAGIC = 2;
    public static final int RECORD_BATCH_OVERHEAD = 8 + 4; // baseOffset + batchLength
    public static final int HEADER_SIZE_UP_TO_RECORDS = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4; // 61 bytes

    private final long baseOffset;
    private final int batchLength;
    private final int partitionLeaderEpoch;
    private final byte magic;
    private final long crc;
    private final short attributes;
    private final int lastOffsetDelta;
    private final long firstTimestamp;
    private final long maxTimestamp;
    private final long producerId;
    private final short producerEpoch;
    private final int baseSequence;
    private final List<Record> records;

    public RecordBatch(
            long baseOffset,
            int batchLength,
            int partitionLeaderEpoch,
            byte magic,
            long crc,
            short attributes,
            int lastOffsetDelta,
            long firstTimestamp,
            long maxTimestamp,
            long producerId,
            short producerEpoch,
            int baseSequence,
            List<Record> records
    ) {
        this.baseOffset = baseOffset;
        this.batchLength = batchLength;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.magic = magic;
        this.crc = crc;
        this.attributes = attributes;
        this.lastOffsetDelta = lastOffsetDelta;
        this.firstTimestamp = firstTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.records = Collections.unmodifiableList(records);
    }

    public long baseOffset() { return baseOffset; }
    public int batchLength() { return batchLength; }
    public int sizeInBytes() { return RECORD_BATCH_OVERHEAD + batchLength; }
    public int partitionLeaderEpoch() { return partitionLeaderEpoch; }
    public byte magic() { return magic; }
    public long crc() { return crc; }
    public short attributes() { return attributes; }
    public int lastOffsetDelta() { return lastOffsetDelta; }
    public long lastOffset() { return baseOffset + lastOffsetDelta; }
    public long firstTimestamp() { return firstTimestamp; }
    public long maxTimestamp() { return maxTimestamp; }
    public long producerId() { return producerId; }
    public short producerEpoch() { return producerEpoch; }
    public int baseSequence() { return baseSequence; }
    public int recordsCount() { return records.size(); }
    public List<Record> records() { return records; }

    public RecordBatch withBaseOffset(long newBaseOffset) {
        List<Record> updatedRecords = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            Record r = records.get(i);
            updatedRecords.add(r.withOffset(newBaseOffset + i));
        }

        return new RecordBatch(
                newBaseOffset,
                batchLength,
                partitionLeaderEpoch,
                magic,
                crc,
                attributes,
                lastOffsetDelta,
                firstTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                updatedRecords
        );
    }

    public CompressionType compressionType() {
        return CompressionType.fromId((byte) (attributes & 0x07));
    }

    /**
     * Serializes this batch into a newly allocated direct or heap ByteBuffer.
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(sizeInBytes());
        writeTo(buf);
        buf.flip();
        return buf;
    }

    public void writeTo(ByteBuffer buffer) {
        int startPos = buffer.position();
        buffer.putLong(baseOffset);
        buffer.putInt(batchLength);

        int crcStart = buffer.position();
        buffer.putInt(partitionLeaderEpoch);
        buffer.put(magic);

        int crcPos = buffer.position();
        buffer.putInt(0); // placeholder for CRC

        int contentStart = buffer.position();
        buffer.putShort(attributes);
        buffer.putInt(lastOffsetDelta);
        buffer.putLong(firstTimestamp);
        buffer.putLong(maxTimestamp);
        buffer.putLong(producerId);
        buffer.putShort(producerEpoch);
        buffer.putInt(baseSequence);
        buffer.putInt(records.size());

        for (Record record : records) {
            writeRecord(buffer, record, baseOffset, firstTimestamp);
        }

        int endPos = buffer.position();
        // Compute CRC32C over content from attributes to endPos
        long computedCrc = ByteUtils.computeCrc32c(buffer, contentStart, endPos - contentStart);
        buffer.putInt(crcPos, (int) (computedCrc & 0xFFFFFFFFL));
        buffer.position(endPos);
    }

    private static void writeRecord(ByteBuffer buf, Record record, long baseOffset, long baseTimestamp) {
        // Calculate record body
        int offsetDelta = (int) (record.offset() >= 0 ? record.offset() - baseOffset : 0);
        long tsDelta = record.timestamp() - baseTimestamp;

        ByteBuffer temp = ByteBuffer.allocate(Math.max(64,
                (record.key() == null ? 0 : record.key().length) +
                (record.value() == null ? 0 : record.value().length) + 64));

        temp.put((byte) 0); // record attributes
        ByteUtils.writeVarlong(tsDelta, temp);
        ByteUtils.writeVarint(offsetDelta, temp);

        if (record.key() == null) {
            ByteUtils.writeVarint(-1, temp);
        } else {
            ByteUtils.writeVarint(record.key().length, temp);
            temp.put(record.key());
        }

        if (record.value() == null) {
            ByteUtils.writeVarint(-1, temp);
        } else {
            ByteUtils.writeVarint(record.value().length, temp);
            temp.put(record.value());
        }

        // Headers
        ByteUtils.writeVarint(record.headers().size(), temp);
        for (Header h : record.headers()) {
            byte[] keyBytes = h.key().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ByteUtils.writeVarint(keyBytes.length, temp);
            temp.put(keyBytes);
            if (h.value() == null) {
                ByteUtils.writeVarint(-1, temp);
            } else {
                ByteUtils.writeVarint(h.value().length, temp);
                temp.put(h.value());
            }
        }

        temp.flip();
        int recordLength = temp.remaining();
        ByteUtils.writeVarint(recordLength, buf);
        buf.put(temp);
    }

    /**
     * Reads a RecordBatch from the buffer.
     */
    public static RecordBatch readFrom(ByteBuffer buffer) {
        if (buffer.remaining() < RECORD_BATCH_OVERHEAD) {
            return null;
        }

        int startPos = buffer.position();
        long baseOffset = buffer.getLong();
        int batchLength = buffer.getInt();

        if (buffer.remaining() < batchLength) {
            // Incomplete batch
            buffer.position(startPos);
            return null;
        }

        int partitionLeaderEpoch = buffer.getInt();
        byte magic = buffer.get();
        long crc = buffer.getInt() & 0xFFFFFFFFL;

        int contentStart = buffer.position();
        short attributes = buffer.getShort();
        int lastOffsetDelta = buffer.getInt();
        long firstTimestamp = buffer.getLong();
        long maxTimestamp = buffer.getLong();
        long producerId = buffer.getLong();
        short producerEpoch = buffer.getShort();
        int baseSequence = buffer.getInt();
        int recordsCount = buffer.getInt();

        List<Record> records = new ArrayList<>(recordsCount);
        for (int i = 0; i < recordsCount; i++) {
            Record rec = readRecord(buffer, baseOffset, firstTimestamp);
            records.add(rec);
        }

        int endPos = buffer.position();
        long computedCrc = ByteUtils.computeCrc32c(buffer, contentStart, endPos - contentStart);
        if (crc != computedCrc) {
            throw new IllegalStateException("CRC32C mismatch: expected " + crc + ", actual " + computedCrc);
        }

        return new RecordBatch(
                baseOffset, batchLength, partitionLeaderEpoch, magic, crc,
                attributes, lastOffsetDelta, firstTimestamp, maxTimestamp,
                producerId, producerEpoch, baseSequence, records
        );
    }

    private static Record readRecord(ByteBuffer buffer, long baseOffset, long baseTimestamp) {
        int recordLength = ByteUtils.readVarint(buffer);
        int startPos = buffer.position();

        byte attributes = buffer.get();
        long tsDelta = ByteUtils.readVarlong(buffer);
        int offsetDelta = ByteUtils.readVarint(buffer);

        int keyLen = ByteUtils.readVarint(buffer);
        byte[] key = null;
        if (keyLen >= 0) {
            key = new byte[keyLen];
            buffer.get(key);
        }

        int valLen = ByteUtils.readVarint(buffer);
        byte[] val = null;
        if (valLen >= 0) {
            val = new byte[valLen];
            buffer.get(val);
        }

        int headersCount = ByteUtils.readVarint(buffer);
        List<Header> headers = new ArrayList<>(headersCount);
        for (int h = 0; h < headersCount; h++) {
            int hKeyLen = ByteUtils.readVarint(buffer);
            byte[] hKey = new byte[hKeyLen];
            buffer.get(hKey);
            int hValLen = ByteUtils.readVarint(buffer);
            byte[] hVal = null;
            if (hValLen >= 0) {
                hVal = new byte[hValLen];
                buffer.get(hVal);
            }
            headers.add(new Header(new String(hKey, java.nio.charset.StandardCharsets.UTF_8), hVal));
        }

        return new Record(baseOffset + offsetDelta, baseTimestamp + tsDelta, key, val, headers);
    }

    public static Builder builder(long baseOffset) {
        return new Builder(baseOffset);
    }

    public static class Builder {
        private final long baseOffset;
        private int leaderEpoch = 0;
        private long producerId = -1L;
        private short producerEpoch = 0;
        private int baseSequence = 0;
        private CompressionType compressionType = CompressionType.NONE;
        private final List<Record> records = new ArrayList<>();

        public Builder(long baseOffset) {
            this.baseOffset = baseOffset;
        }

        public Builder leaderEpoch(int epoch) {
            this.leaderEpoch = epoch;
            return this;
        }

        public Builder producer(long producerId, short producerEpoch, int baseSequence) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.baseSequence = baseSequence;
            return this;
        }

        public Builder compression(CompressionType type) {
            this.compressionType = type;
            return this;
        }

        public Builder append(Record record) {
            long recordOffset = baseOffset + records.size();
            records.add(record.withOffset(recordOffset));
            return this;
        }

        public Builder append(String key, String value) {
            return append(new Record(key, value));
        }

        public Builder append(byte[] key, byte[] value) {
            return append(new Record(key, value));
        }

        public RecordBatch build() {
            if (records.isEmpty()) {
                throw new IllegalStateException("Cannot build empty RecordBatch");
            }

            long firstTs = records.get(0).timestamp();
            long maxTs = firstTs;
            for (Record r : records) {
                if (r.timestamp() > maxTs) {
                    maxTs = r.timestamp();
                }
            }

            int lastOffsetDelta = records.size() - 1;
            short attributes = (short) (compressionType.id() & 0x07);

            // Compute size
            // Header after batchLength: 4(epoch) + 1(magic) + 4(crc) + 2(attrs) + 4(delta) + 8(ts) + 8(maxTs) + 8(pid) + 2(pepoch) + 4(seq) + 4(count) = 49 bytes
            int bodyLength = 49;
            for (Record r : records) {
                bodyLength += estimatedRecordSize(r);
            }

            // Create temporary buffer to serialize and calculate exact length and CRC
            ByteBuffer temp = ByteBuffer.allocate(RECORD_BATCH_OVERHEAD + bodyLength + 1024);
            int batchLengthPos = 8;
            temp.position(RECORD_BATCH_OVERHEAD);

            int crcStart = temp.position();
            temp.putInt(leaderEpoch);
            temp.put(CURRENT_MAGIC);
            int crcPos = temp.position();
            temp.putInt(0);

            int contentStart = temp.position();
            temp.putShort(attributes);
            temp.putInt(lastOffsetDelta);
            temp.putLong(firstTs);
            temp.putLong(maxTs);
            temp.putLong(producerId);
            temp.putShort(producerEpoch);
            temp.putInt(baseSequence);
            temp.putInt(records.size());

            for (Record r : records) {
                writeRecord(temp, r, baseOffset, firstTs);
            }

            int endPos = temp.position();
            int actualBatchLength = endPos - RECORD_BATCH_OVERHEAD;

            long crc = ByteUtils.computeCrc32c(temp, contentStart, endPos - contentStart);

            return new RecordBatch(
                    baseOffset,
                    actualBatchLength,
                    leaderEpoch,
                    CURRENT_MAGIC,
                    crc,
                    attributes,
                    lastOffsetDelta,
                    firstTs,
                    maxTs,
                    producerId,
                    producerEpoch,
                    baseSequence,
                    records
            );
        }

        private static int estimatedRecordSize(Record r) {
            return 16 + (r.key() == null ? 1 : r.key().length + 5) +
                    (r.value() == null ? 1 : r.value().length + 5) + 32;
        }
    }
}
