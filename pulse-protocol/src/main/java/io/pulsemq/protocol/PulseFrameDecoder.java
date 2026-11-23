package io.pulsemq.protocol;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Frames incoming raw TCP byte streams into discrete 4-byte length-prefixed messages.
 */
public class PulseFrameDecoder extends LengthFieldBasedFrameDecoder {

    public static final int MAX_FRAME_LENGTH = 128 * 1024 * 1024; // 128 MB

    public PulseFrameDecoder() {
        super(MAX_FRAME_LENGTH, 0, 4, 0, 4);
    }

    public PulseFrameDecoder(int maxFrameLength) {
        super(maxFrameLength, 0, 4, 0, 4);
    }
}
