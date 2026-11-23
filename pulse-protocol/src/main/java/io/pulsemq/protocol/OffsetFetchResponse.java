package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public record OffsetFetchResponse(
        ResponseHeader header,
        Map<TopicPartition, PartitionOffsetFetchData> responses
) implements PulseResponse {

    public record PartitionOffsetFetchData(
            long offset,
            String metadata,
            ErrorCode errorCode
    ) {}

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(responses.size());
        for (Map.Entry<TopicPartition, PartitionOffsetFetchData> entry : responses.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetCommitRequest.writeString(out, tp.topic());
            out.writeInt(tp.partition());

            PartitionOffsetFetchData data = entry.getValue();
            out.writeLong(data.offset());
            OffsetCommitRequest.writeString(out, data.metadata());
            out.writeShort(data.errorCode().code());
        }
    }

    public static OffsetFetchResponse readFrom(ResponseHeader header, ByteBuf in) {
        int count = in.readInt();
        Map<TopicPartition, PartitionOffsetFetchData> responses = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            String topic = OffsetCommitRequest.readString(in);
            int partition = in.readInt();
            long offset = in.readLong();
            String metadata = OffsetCommitRequest.readString(in);
            short errCode = in.readShort();

            responses.put(
                    new TopicPartition(topic, partition),
                    new PartitionOffsetFetchData(offset, metadata, ErrorCode.fromCode(errCode))
            );
        }
        return new OffsetFetchResponse(header, responses);
    }
}
