package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pulsemq.common.ErrorCode;
import io.pulsemq.common.RecordBatch;
import io.pulsemq.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PulseProtocolTest {

    @Test
    void testProduceRequestRoundTrip() {
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 1, 42, "test-client");
        TopicPartition tp = new TopicPartition("sensors", 0);
        RecordBatch batch = RecordBatch.builder(0L).append("k1", "v1").build();

        ProduceRequest req = new ProduceRequest(header, (short) 1, 5000, Map.of(tp, batch));

        ByteBuf buf = Unpooled.buffer();
        PulseCodec.encodeRequest(req, buf);

        // Strip length prefix as PulseFrameDecoder would
        int length = buf.readInt();
        assertThat(length).isEqualTo(buf.readableBytes());

        PulseRequest decoded = PulseCodec.decodeRequest(buf);
        assertThat(decoded).isInstanceOf(ProduceRequest.class);
        ProduceRequest pDecoded = (ProduceRequest) decoded;
        assertThat(pDecoded.header().correlationId()).isEqualTo(42);
        assertThat(pDecoded.acks()).isEqualTo((short) 1);
        assertThat(pDecoded.partitionData()).containsKey(tp);
        RecordBatch decodedBatch = pDecoded.partitionData().get(tp);
        assertThat(decodedBatch.recordsCount()).isEqualTo(1);
        assertThat(decodedBatch.records().get(0).keyAsString()).isEqualTo("k1");
    }

    @Test
    void testMetadataResponseRoundTrip() {
        ResponseHeader header = new ResponseHeader(99);
        List<MetadataResponse.BrokerNode> brokers = List.of(
                new MetadataResponse.BrokerNode(1, "127.0.0.1", 9092)
        );
        List<MetadataResponse.TopicMetadata> topics = List.of(
                new MetadataResponse.TopicMetadata(
                        "orders",
                        ErrorCode.NONE,
                        List.of(new MetadataResponse.PartitionMetadata(0, 1, List.of(1), List.of(1), ErrorCode.NONE))
                )
        );

        MetadataResponse resp = new MetadataResponse(header, brokers, topics);
        ByteBuf buf = Unpooled.buffer();
        PulseCodec.encodeResponse(resp, buf);

        int length = buf.readInt();
        assertThat(length).isEqualTo(buf.readableBytes());

        PulseResponse decoded = PulseCodec.decodeResponse(buf, ApiKeys.METADATA);
        assertThat(decoded).isInstanceOf(MetadataResponse.class);
        MetadataResponse mDecoded = (MetadataResponse) decoded;
        assertThat(mDecoded.header().correlationId()).isEqualTo(99);
        assertThat(mDecoded.brokers()).hasSize(1);
        assertThat(mDecoded.brokers().get(0).host()).isEqualTo("127.0.0.1");
        assertThat(mDecoded.topicMetadata()).hasSize(1);
        assertThat(mDecoded.topicMetadata().get(0).topic()).isEqualTo("orders");
    }

    @Test
    void testCreateTopicsRoundTrip() {
        RequestHeader header = new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, 101, "admin-cli");
        CreateTopicsRequest req = new CreateTopicsRequest(
                header,
                Map.of("payments", new CreateTopicsRequest.TopicDetails(3, (short) 2)),
                5000
        );

        ByteBuf buf = Unpooled.buffer();
        PulseCodec.encodeRequest(req, buf);
        int len = buf.readInt();
        assertThat(len).isEqualTo(buf.readableBytes());

        PulseRequest decoded = PulseCodec.decodeRequest(buf);
        assertThat(decoded).isInstanceOf(CreateTopicsRequest.class);
        CreateTopicsRequest cReq = (CreateTopicsRequest) decoded;
        assertThat(cReq.topics()).containsKey("payments");
        assertThat(cReq.topics().get("payments").numPartitions()).isEqualTo(3);
        assertThat(cReq.topics().get("payments").replicationFactor()).isEqualTo((short) 2);

        ResponseHeader rHeader = new ResponseHeader(101);
        CreateTopicsResponse resp = new CreateTopicsResponse(rHeader, Map.of("payments", ErrorCode.NONE));
        ByteBuf respBuf = Unpooled.buffer();
        PulseCodec.encodeResponse(resp, respBuf);
        int respLen = respBuf.readInt();
        assertThat(respLen).isEqualTo(respBuf.readableBytes());

        PulseResponse decodedResp = PulseCodec.decodeResponse(respBuf, ApiKeys.CREATE_TOPICS);
        assertThat(decodedResp).isInstanceOf(CreateTopicsResponse.class);
        assertThat(((CreateTopicsResponse) decodedResp).results().get("payments")).isEqualTo(ErrorCode.NONE);
    }
}
