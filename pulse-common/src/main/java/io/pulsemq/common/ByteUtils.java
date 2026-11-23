package io.pulsemq.common;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * Utility functions for byte buffer manipulation, varints, and CRC32C checksums.
 */
public final class ByteUtils {

    private ByteUtils() {}

    /**
     * Compute CRC32C of a byte buffer slice between offset and length.
     */
    public static long computeCrc32c(ByteBuffer buffer, int offset, int length) {
        CRC32C crc = new CRC32C();
        int oldPos = buffer.position();
        int oldLimit = buffer.limit();
        try {
            buffer.position(offset);
            buffer.limit(offset + length);
            crc.update(buffer);
            return crc.getValue();
        } finally {
            buffer.position(oldPos);
            buffer.limit(oldLimit);
        }
    }

    public static long computeCrc32c(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return crc.getValue();
    }

    /**
     * Write an unsigned varint into a ByteBuffer.
     */
    public static void writeVarint(int value, ByteBuffer buffer) {
        while ((value & 0xFFFFFF80) != 0L) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) (value & 0x7F));
    }

    /**
     * Read an unsigned varint from a ByteBuffer.
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 28) {
                throw new IllegalArgumentException("Invalid varint format: exceeds 32 bits");
            }
        }
        value |= b << i;
        return value;
    }

    /**
     * Write a signed varlong into a ByteBuffer using ZigZag encoding.
     */
    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
            buffer.put((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        buffer.put((byte) (v & 0x7F));
    }

    /**
     * Read a signed varlong from a ByteBuffer with ZigZag decoding.
     */
    public static long readVarlong(ByteBuffer buffer) {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Invalid varlong format: exceeds 64 bits");
            }
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }
}
