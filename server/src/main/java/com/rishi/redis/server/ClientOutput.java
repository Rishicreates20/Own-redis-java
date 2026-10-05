package com.rishi.redis.server;

import com.rishi.redis.protocol.RespValue;

/**
 * Where a session's out-of-band replies go.
 *
 * <p>Ordinary command replies are simply returned by the handler, but Pub/Sub pushes
 * are produced by <em>another</em> client's thread and must reach this connection
 * whenever they happen. Abstracting the sink keeps the Pub/Sub registry ignorant of
 * whether a subscriber is a RESP socket or an HTTP session parked on a poll.
 */
public interface ClientOutput {

    /** Pushes a value to the client. Must be safe to call from any thread. */
    void push(RespValue value);

    /** False once the underlying connection is gone, so the registry can drop it. */
    boolean isOpen();

    String remoteAddress();

    /** A sink that drops everything; used by internal/administrative sessions. */
    ClientOutput DISCARDING = new ClientOutput() {
        @Override
        public void push(RespValue value) {
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public String remoteAddress() {
            return "internal";
        }
    };
}
