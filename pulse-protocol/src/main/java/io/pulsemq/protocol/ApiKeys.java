package io.pulsemq.protocol;

/**
 * Protocol API identifiers.
 */
public enum ApiKeys {
    PRODUCE((short) 0, "Produce"),
    FETCH((short) 1, "Fetch"),
    METADATA((short) 2, "Metadata"),
    OFFSET_COMMIT((short) 3, "OffsetCommit"),
    OFFSET_FETCH((short) 4, "OffsetFetch"),
    JOIN_GROUP((short) 5, "JoinGroup"),
    SYNC_GROUP((short) 6, "SyncGroup"),
    HEARTBEAT((short) 7, "Heartbeat"),
    LEAVE_GROUP((short) 8, "LeaveGroup"),
    CREATE_TOPICS((short) 9, "CreateTopics"),
    DELETE_TOPICS((short) 10, "DeleteTopics");

    private final short id;
    private final String name;

    ApiKeys(short id, String name) {
        this.id = id;
        this.name = name;
    }

    public short id() {
        return id;
    }

    public String apiName() {
        return name;
    }

    public static ApiKeys fromId(short id) {
        for (ApiKeys key : values()) {
            if (key.id == id) {
                return key;
            }
        }
        throw new IllegalArgumentException("Unknown ApiKey: " + id);
    }
}
