package io.pulsemq.consensus;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pulsemq.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Event-sourced metadata log record types replicating cluster state via Raft.
 */
public sealed interface MetadataRecord {

    byte type();

    record RegisterBrokerRecord(
            int brokerId,
            String host,
            int port,
            long registeredTime,
            boolean fenced
    ) implements MetadataRecord {
        @Override
        public byte type() { return 1; }
    }

    record TopicRecord(
            String topicName,
            String topicId
    ) implements MetadataRecord {
        @Override
        public byte type() { return 2; }
    }

    record PartitionRecord(
            String topic,
            int partitionId,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicas,
            List<Integer> isr
    ) implements MetadataRecord {
        @Override
        public byte type() { return 3; }
    }

    record PartitionChangeRecord(
            String topic,
            int partitionId,
            int newLeaderId,
            int newLeaderEpoch,
            List<Integer> newIsr
    ) implements MetadataRecord {
        @Override
        public byte type() { return 4; }
    }

    record RemoveTopicRecord(
            String topicName
    ) implements MetadataRecord {
        @Override
        public byte type() { return 5; }
    }

    static byte[] serialize(MetadataRecord record) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(record.type());

        switch (record) {
            case RegisterBrokerRecord r -> {
                buf.writeInt(r.brokerId());
                writeString(buf, r.host());
                buf.writeInt(r.port());
                buf.writeLong(r.registeredTime());
                buf.writeBoolean(r.fenced());
            }
            case TopicRecord r -> {
                writeString(buf, r.topicName());
                writeString(buf, r.topicId());
            }
            case PartitionRecord r -> {
                writeString(buf, r.topic());
                buf.writeInt(r.partitionId());
                buf.writeInt(r.leaderId());
                buf.writeInt(r.leaderEpoch());
                buf.writeInt(r.replicas().size());
                for (int rep : r.replicas()) buf.writeInt(rep);
                buf.writeInt(r.isr().size());
                for (int isrId : r.isr()) buf.writeInt(isrId);
            }
            case PartitionChangeRecord r -> {
                writeString(buf, r.topic());
                buf.writeInt(r.partitionId());
                buf.writeInt(r.newLeaderId());
                buf.writeInt(r.newLeaderEpoch());
                buf.writeInt(r.newIsr().size());
                for (int isrId : r.newIsr()) buf.writeInt(isrId);
            }
            case RemoveTopicRecord r -> {
                writeString(buf, r.topicName());
            }
        }

        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }

    static MetadataRecord deserialize(byte[] data) {
        ByteBuf in = Unpooled.wrappedBuffer(data);
        byte type = in.readByte();

        return switch (type) {
            case 1 -> {
                int brokerId = in.readInt();
                String host = readString(in);
                int port = in.readInt();
                long registeredTime = in.readLong();
                boolean fenced = in.readBoolean();
                yield new RegisterBrokerRecord(brokerId, host, port, registeredTime, fenced);
            }
            case 2 -> {
                String topicName = readString(in);
                String topicId = readString(in);
                yield new TopicRecord(topicName, topicId);
            }
            case 3 -> {
                String topic = readString(in);
                int partitionId = in.readInt();
                int leaderId = in.readInt();
                int leaderEpoch = in.readInt();
                int repCount = in.readInt();
                List<Integer> replicas = new ArrayList<>(repCount);
                for (int i = 0; i < repCount; i++) replicas.add(in.readInt());
                int isrCount = in.readInt();
                List<Integer> isr = new ArrayList<>(isrCount);
                for (int i = 0; i < isrCount; i++) isr.add(in.readInt());
                yield new PartitionRecord(topic, partitionId, leaderId, leaderEpoch, replicas, isr);
            }
            case 4 -> {
                String topic = readString(in);
                int partitionId = in.readInt();
                int newLeaderId = in.readInt();
                int newLeaderEpoch = in.readInt();
                int isrCount = in.readInt();
                List<Integer> newIsr = new ArrayList<>(isrCount);
                for (int i = 0; i < isrCount; i++) newIsr.add(in.readInt());
                yield new PartitionChangeRecord(topic, partitionId, newLeaderId, newLeaderEpoch, newIsr);
            }
            case 5 -> {
                String topicName = readString(in);
                yield new RemoveTopicRecord(topicName);
            }
            default -> throw new IllegalArgumentException("Unknown MetadataRecord type: " + type);
        };
    }

    private static void writeString(ByteBuf buf, String str) {
        if (str == null) {
            buf.writeShort(-1);
        } else {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            buf.writeShort(bytes.length);
            buf.writeBytes(bytes);
        }
    }

    private static String readString(ByteBuf buf) {
        short len = buf.readShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
