package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record MetadataResponse(
        ResponseHeader header,
        List<BrokerNode> brokers,
        List<TopicMetadata> topicMetadata
) implements PulseResponse {

    public record BrokerNode(int nodeId, String host, int port) {}

    public record PartitionMetadata(
            int partitionId,
            int leaderId,
            List<Integer> replicas,
            List<Integer> isr,
            ErrorCode errorCode
    ) {}

    public record TopicMetadata(
            String topic,
            ErrorCode errorCode,
            List<PartitionMetadata> partitions
    ) {}

    @Override
    public void writeBody(ByteBuf out) {
        // Write brokers
        out.writeInt(brokers.size());
        for (BrokerNode node : brokers) {
            out.writeInt(node.nodeId());
            byte[] hostBytes = node.host().getBytes(StandardCharsets.UTF_8);
            out.writeShort(hostBytes.length);
            out.writeBytes(hostBytes);
            out.writeInt(node.port());
        }

        // Write topics
        out.writeInt(topicMetadata.size());
        for (TopicMetadata tm : topicMetadata) {
            byte[] topicBytes = tm.topic().getBytes(StandardCharsets.UTF_8);
            out.writeShort(topicBytes.length);
            out.writeBytes(topicBytes);
            out.writeShort(tm.errorCode().code());

            out.writeInt(tm.partitions().size());
            for (PartitionMetadata pm : tm.partitions()) {
                out.writeInt(pm.partitionId());
                out.writeInt(pm.leaderId());
                out.writeShort(pm.errorCode().code());

                out.writeInt(pm.replicas().size());
                for (int rep : pm.replicas()) {
                    out.writeInt(rep);
                }

                out.writeInt(pm.isr().size());
                for (int isrId : pm.isr()) {
                    out.writeInt(isrId);
                }
            }
        }
    }

    public static MetadataResponse readFrom(ResponseHeader header, ByteBuf in) {
        int brokerCount = in.readInt();
        List<BrokerNode> brokers = new ArrayList<>(brokerCount);
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = in.readInt();
            short hostLen = in.readShort();
            byte[] hostBytes = new byte[hostLen];
            in.readBytes(hostBytes);
            int port = in.readInt();
            brokers.add(new BrokerNode(nodeId, new String(hostBytes, StandardCharsets.UTF_8), port));
        }

        int topicCount = in.readInt();
        List<TopicMetadata> topics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            short tLen = in.readShort();
            byte[] tBytes = new byte[tLen];
            in.readBytes(tBytes);
            String topic = new String(tBytes, StandardCharsets.UTF_8);
            short errCode = in.readShort();

            int partCount = in.readInt();
            List<PartitionMetadata> partitions = new ArrayList<>(partCount);
            for (int p = 0; p < partCount; p++) {
                int partId = in.readInt();
                int leaderId = in.readInt();
                short partErr = in.readShort();

                int repCount = in.readInt();
                List<Integer> replicas = new ArrayList<>(repCount);
                for (int r = 0; r < repCount; r++) {
                    replicas.add(in.readInt());
                }

                int isrCount = in.readInt();
                List<Integer> isr = new ArrayList<>(isrCount);
                for (int is = 0; is < isrCount; is++) {
                    isr.add(in.readInt());
                }

                partitions.add(new PartitionMetadata(partId, leaderId, replicas, isr, ErrorCode.fromCode(partErr)));
            }
            topics.add(new TopicMetadata(topic, ErrorCode.fromCode(errCode), partitions));
        }

        return new MetadataResponse(header, brokers, topics);
    }
}
