package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Base interface for all response messages.
 */
public interface PulseResponse {
    ResponseHeader header();
    void writeBody(ByteBuf out);
}
