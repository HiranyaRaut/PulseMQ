package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public record FetchRequest(
        RequestHeader header,
        int maxWaitMs,
        int minBytes,
        int maxBytes,
        Map<TopicPartition, PartitionFetchData> partitionData
) implements PulseRequest {

    public record PartitionFetchData(long fetchOffset, int maxBytes) {}

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(maxWaitMs);
        out.writeInt(minBytes);
        out.writeInt(maxBytes);
        out.writeInt(partitionData.size());

        for (Map.Entry<TopicPartition, PartitionFetchData> entry : partitionData.entrySet()) {
            TopicPartition tp = entry.getKey();
            byte[] topicBytes = tp.topic().getBytes(StandardCharsets.UTF_8);
            out.writeShort(topicBytes.length);
            out.writeBytes(topicBytes);
            out.writeInt(tp.partition());

            PartitionFetchData pfd = entry.getValue();
            out.writeLong(pfd.fetchOffset());
            out.writeInt(pfd.maxBytes());
        }
    }

    public static FetchRequest readFrom(RequestHeader header, ByteBuf in) {
        int maxWaitMs = in.readInt();
        int minBytes = in.readInt();
        int maxBytes = in.readInt();
        int numPartitions = in.readInt();

        Map<TopicPartition, PartitionFetchData> partitionData = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            short topicLen = in.readShort();
            byte[] topicBytes = new byte[topicLen];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);
            int partition = in.readInt();

            long fetchOffset = in.readLong();
            int partMaxBytes = in.readInt();

            partitionData.put(new TopicPartition(topic, partition), new PartitionFetchData(fetchOffset, partMaxBytes));
        }

        return new FetchRequest(header, maxWaitMs, minBytes, maxBytes, partitionData);
    }
}
