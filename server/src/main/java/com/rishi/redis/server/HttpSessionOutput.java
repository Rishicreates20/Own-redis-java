package com.rishi.redis.server;

import com.rishi.redis.protocol.RespValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The push sink for a browser session.
 *
 * <p>HTTP has no way to hand a client an unsolicited frame, so Pub/Sub messages
 * addressed to a Web CLI session are parked in a bounded queue and delivered with the
 * next response — either appended to the reply of the client's next command, or
 * collected by a poll of {@code /api/messages}. The bound matters: without it, a tab
 * that subscribes and is then closed would pin every message published afterwards.
 */
public final class HttpSessionOutput implements ClientOutput {

    private static final int MAX_PENDING = 256;

    private final Queue<RespValue> pending = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger();
    private final String remoteAddress;
    private volatile boolean open = true;

    public HttpSessionOutput(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void push(RespValue value) {
        if (size.get() >= MAX_PENDING) {
            // Drop the oldest so a subscriber that stopped polling degrades instead of
            // growing without bound.
            if (pending.poll() != null) {
                size.decrementAndGet();
            }
        }
        pending.add(value);
        size.incrementAndGet();
    }

    /** Removes and returns everything queued since the last drain. */
    public List<RespValue> drain() {
        List<RespValue> drained = new ArrayList<>();
        RespValue value;
        while ((value = pending.poll()) != null) {
            size.decrementAndGet();
            drained.add(value);
        }
        return drained;
    }

    public int pendingCount() {
        return size.get();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    public void close() {
        open = false;
        pending.clear();
        size.set(0);
    }

    @Override
    public String remoteAddress() {
        return remoteAddress;
    }
}
