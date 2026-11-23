package io.pulsemq.common;

public enum CompressionType {
    NONE((byte) 0),
    GZIP((byte) 1),
    SNAPPY((byte) 2),
    LZ4((byte) 3);

    private final byte id;

    CompressionType(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static CompressionType fromId(byte id) {
        for (CompressionType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown compression type id: " + id);
    }
}
