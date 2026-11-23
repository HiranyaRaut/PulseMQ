package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

public record OffsetFetchRequest(
        RequestHeader header,
        String groupId,
        List<TopicPartition> partitions
) implements PulseRequest {

    @Override
    public void writeBody(ByteBuf out) {
        OffsetCommitRequest.writeString(out, groupId);
        out.writeInt(partitions.size());
        for (TopicPartition tp : partitions) {
            OffsetCommitRequest.writeString(out, tp.topic());
            out.writeInt(tp.partition());
        }
    }

    public static OffsetFetchRequest readFrom(RequestHeader header, ByteBuf in) {
        String groupId = OffsetCommitRequest.readString(in);
        int count = in.readInt();
        List<TopicPartition> partitions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String topic = OffsetCommitRequest.readString(in);
            int partition = in.readInt();
            partitions.add(new TopicPartition(topic, partition));
        }
        return new OffsetFetchRequest(header, groupId, partitions);
    }
}
