package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public record OffsetCommitRequest(
        RequestHeader header,
        String groupId,
        int generationId,
        String memberId,
        Map<TopicPartition, PartitionCommitData> offsets
) implements PulseRequest {

    public record PartitionCommitData(long offset, String metadata) {}

    @Override
    public void writeBody(ByteBuf out) {
        writeString(out, groupId);
        out.writeInt(generationId);
        writeString(out, memberId);

        out.writeInt(offsets.size());
        for (Map.Entry<TopicPartition, PartitionCommitData> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            writeString(out, tp.topic());
            out.writeInt(tp.partition());

            PartitionCommitData pcd = entry.getValue();
            out.writeLong(pcd.offset());
            writeString(out, pcd.metadata());
        }
    }

    public static OffsetCommitRequest readFrom(RequestHeader header, ByteBuf in) {
        String groupId = readString(in);
        int generationId = in.readInt();
        String memberId = readString(in);

        int count = in.readInt();
        Map<TopicPartition, PartitionCommitData> offsets = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            String topic = readString(in);
            int partition = in.readInt();
            long offset = in.readLong();
            String metadata = readString(in);
            offsets.put(new TopicPartition(topic, partition), new PartitionCommitData(offset, metadata));
        }

        return new OffsetCommitRequest(header, groupId, generationId, memberId, offsets);
    }

    static void writeString(ByteBuf out, String str) {
        if (str == null) {
            out.writeShort(-1);
        } else {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            out.writeShort(bytes.length);
            out.writeBytes(bytes);
        }
    }

    static String readString(ByteBuf in) {
        short len = in.readShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        in.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
