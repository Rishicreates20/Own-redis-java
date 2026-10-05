package com.rishi.redis.protocol;

/**
 * A malformed RESP frame. Unrecoverable for the connection: the parser cannot
 * know where the next frame begins, so the server replies with the error and
 * closes the channel (this is what real Redis does too).
 */
public class RespProtocolException extends RuntimeException {

    public RespProtocolException(String message) {
        super(message, null, false, false);
    }
}
