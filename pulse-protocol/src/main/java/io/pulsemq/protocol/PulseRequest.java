package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Base interface for all request messages.
 */
public interface PulseRequest {
    RequestHeader header();
    void writeBody(ByteBuf out);
}
