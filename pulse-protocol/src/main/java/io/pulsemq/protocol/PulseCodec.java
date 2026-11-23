package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Serialization and deserialization utilities for PulseMQ network protocol.
 */
public final class PulseCodec {

    private PulseCodec() {}

    public static void encodeRequest(PulseRequest request, ByteBuf out) {
        int lengthPos = out.writerIndex();
        out.writeInt(0); // length placeholder

        int bodyStart = out.writerIndex();
        request.header().writeTo(out);
        request.writeBody(out);

        int frameLength = out.writerIndex() - bodyStart;
        out.setInt(lengthPos, frameLength);
    }

    public static PulseRequest decodeRequest(ByteBuf in) {
        RequestHeader header = RequestHeader.readFrom(in);
        return switch (header.apiKey()) {
            case PRODUCE -> ProduceRequest.readFrom(header, in);
            case FETCH -> FetchRequest.readFrom(header, in);
            case METADATA -> MetadataRequest.readFrom(header, in);
            case OFFSET_COMMIT -> OffsetCommitRequest.readFrom(header, in);
            case OFFSET_FETCH -> OffsetFetchRequest.readFrom(header, in);
            case JOIN_GROUP -> JoinGroupRequest.readFrom(header, in);
            case HEARTBEAT -> HeartbeatRequest.readFrom(header, in);
            default -> throw new UnsupportedOperationException("Unsupported Request ApiKey: " + header.apiKey());
        };
    }

    public static void encodeResponse(PulseResponse response, ByteBuf out) {
        int lengthPos = out.writerIndex();
        out.writeInt(0); // length placeholder

        int bodyStart = out.writerIndex();
        response.header().writeTo(out);
        response.writeBody(out);

        int frameLength = out.writerIndex() - bodyStart;
        out.setInt(lengthPos, frameLength);
    }

    public static PulseResponse decodeResponse(ByteBuf in, ApiKeys apiKey) {
        ResponseHeader header = ResponseHeader.readFrom(in);
        return switch (apiKey) {
            case PRODUCE -> ProduceResponse.readFrom(header, in);
            case FETCH -> FetchResponse.readFrom(header, in);
            case METADATA -> MetadataResponse.readFrom(header, in);
            case OFFSET_COMMIT -> OffsetCommitResponse.readFrom(header, in);
            case OFFSET_FETCH -> OffsetFetchResponse.readFrom(header, in);
            case JOIN_GROUP -> JoinGroupResponse.readFrom(header, in);
            case HEARTBEAT -> HeartbeatResponse.readFrom(header, in);
            default -> throw new UnsupportedOperationException("Unsupported Response ApiKey: " + apiKey);
        };
    }
}
