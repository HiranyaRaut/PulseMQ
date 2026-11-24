package io.pulsemq.common;

/**
 * Standard PulseMQ wire error codes.
 */
public enum ErrorCode {
    NONE((short) 0, "No error"),
    UNKNOWN_SERVER_ERROR((short) 1, "An unexpected server error occurred"),
    OFFSET_OUT_OF_BOUNDS((short) 2, "Requested offset is out of range for partition"),
    CORRUPT_MESSAGE((short) 3, "Message CRC checksum failed or format is invalid"),
    UNKNOWN_TOPIC_OR_PARTITION((short) 4, "Topic or partition does not exist on broker"),
    NOT_LEADER_OR_FOLLOWER((short) 5, "Broker is not the leader or replica for partition"),
    REQUEST_TIMED_OUT((short) 6, "Request timed out"),
    NOT_ENOUGH_REPLICAS((short) 7, "Number of in-sync replicas is below min.insync.replicas threshold"),
    REBALANCE_IN_PROGRESS((short) 8, "Consumer group is currently rebalancing"),
    UNKNOWN_MEMBER_ID((short) 9, "Member ID not recognized by group coordinator"),
    ILLEGAL_GENERATION((short) 10, "Group generation ID does not match current group state"),
    INCONSISTENT_GROUP_PROTOCOL((short) 11, "Protocol type or name inconsistent in group"),
    INVALID_TOPIC_EXCEPTION((short) 12, "Topic name violates naming convention or is reserved"),
    MESSAGE_TOO_LARGE((short) 13, "Batch or record exceeds configured maximum size"),
    LEADER_NOT_AVAILABLE((short) 14, "Partition leader is not currently available"),
    TOPIC_ALREADY_EXISTS((short) 15, "Topic with this name already exists in cluster"),
    INVALID_REPLICATION_FACTOR((short) 16, "Replication factor is invalid or exceeds available brokers");

    private final short code;
    private final String description;

    ErrorCode(short code, String description) {
        this.code = code;
        this.description = description;
    }

    public short code() {
        return code;
    }

    public String description() {
        return description;
    }

    public static ErrorCode fromCode(short code) {
        for (ErrorCode ec : values()) {
            if (ec.code == code) {
                return ec;
            }
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
