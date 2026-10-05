package com.rishi.redis.server;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * TD-3 — serialises one connection's commands over a shared executor.
 *
 * <p>Handing every command straight to a virtual-thread pool would let two pipelined
 * commands from the same client run concurrently and reply out of order — a client
 * that sends {@code SET k 1} then {@code GET k} would sometimes see the old value.
 * This wrapper keeps a per-connection FIFO and only ever has one task in flight, so
 * commands stay ordered while still running off the event loop.
 */
public final class SerialExecutor implements Executor {

    private final Queue<Runnable> pending = new ArrayDeque<>();
    private final Executor delegate;
    private Runnable active;

    public SerialExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Runnable task) {
        Runnable start;
        synchronized (this) {
            pending.add(() -> {
                try {
                    task.run();
                } finally {
                    scheduleNext();
                }
            });
            // Claim the in-flight slot under the lock, or two submitters racing here
            // would both start a task and break ordering.
            if (active != null) {
                return;
            }
            active = pending.poll();
            start = active;
        }
        if (start != null) {
            delegate.execute(start);
        }
    }

    private void scheduleNext() {
        Runnable next;
        synchronized (this) {
            active = pending.poll();
            next = active;
        }
        if (next != null) {
            delegate.execute(next);
        }
    }
}
