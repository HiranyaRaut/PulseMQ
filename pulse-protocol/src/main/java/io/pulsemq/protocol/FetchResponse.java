package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public record FetchResponse(
        ResponseHeader header,
        Map<TopicPartition, PartitionFetchResult> responses
) implements PulseResponse {

    public record PartitionFetchResult(
            ErrorCode errorCode,
            long highWatermark,
            byte[] recordsData
    ) {}

    @Override
    public void writeBody(ByteBuf out) {
        out.writeInt(responses.size());
        for (Map.Entry<TopicPartition, PartitionFetchResult> entry : responses.entrySet()) {
            TopicPartition tp = entry.getKey();
            byte[] topicBytes = tp.topic().getBytes(StandardCharsets.UTF_8);
            out.writeShort(topicBytes.length);
            out.writeBytes(topicBytes);
            out.writeInt(tp.partition());

            PartitionFetchResult res = entry.getValue();
            out.writeShort(res.errorCode().code());
            out.writeLong(res.highWatermark());

            if (res.recordsData() == null || res.recordsData().length == 0) {
                out.writeInt(0);
            } else {
                out.writeInt(res.recordsData().length);
                out.writeBytes(res.recordsData());
            }
        }
    }

    public static FetchResponse readFrom(ResponseHeader header, ByteBuf in) {
        int count = in.readInt();
        Map<TopicPartition, PartitionFetchResult> responses = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            short topicLen = in.readShort();
            byte[] topicBytes = new byte[topicLen];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);
            int partition = in.readInt();

            short errCode = in.readShort();
            long highWatermark = in.readLong();
            int recordsLen = in.readInt();
            byte[] recordsData = null;
            if (recordsLen > 0) {
                recordsData = new byte[recordsLen];
                in.readBytes(recordsData);
            }

            responses.put(
                    new TopicPartition(topic, partition),
                    new PartitionFetchResult(ErrorCode.fromCode(errCode), highWatermark, recordsData)
            );
        }
        return new FetchResponse(header, responses);
    }
}
