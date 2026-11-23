package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * Standard PulseMQ request frame header:
 * [apiKey: 2B] [apiVersion: 2B] [correlationId: 4B] [clientIdLength: 2B] [clientId: bytes]
 */
public record RequestHeader(
        ApiKeys apiKey,
        short apiVersion,
        int correlationId,
        String clientId
) {
    public void writeTo(ByteBuf out) {
        out.writeShort(apiKey.id());
        out.writeShort(apiVersion);
        out.writeInt(correlationId);
        if (clientId == null) {
            out.writeShort(-1);
        } else {
            byte[] bytes = clientId.getBytes(StandardCharsets.UTF_8);
            out.writeShort(bytes.length);
            out.writeBytes(bytes);
        }
    }

    public static RequestHeader readFrom(ByteBuf in) {
        short apiKeyId = in.readShort();
        short apiVersion = in.readShort();
        int correlationId = in.readInt();
        short clientIdLen = in.readShort();
        String clientId = null;
        if (clientIdLen >= 0) {
            byte[] bytes = new byte[clientIdLen];
            in.readBytes(bytes);
            clientId = new String(bytes, StandardCharsets.UTF_8);
        }
        return new RequestHeader(ApiKeys.fromId(apiKeyId), apiVersion, correlationId, clientId);
    }
}
