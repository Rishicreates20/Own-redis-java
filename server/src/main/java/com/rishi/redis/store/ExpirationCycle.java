package com.rishi.redis.store;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TD-6 / FR-6 — the active half of expiration.
 *
 * <p>Lazy expiration alone is not enough: a key with a 1-second TTL that nobody ever
 * reads again would sit in memory forever. So a background thread runs a cycle over
 * each database: sample ~20 keys that carry a TTL, delete the dead ones, and if more
 * than 25% of the sample was dead, immediately sample again — the keyspace is clearly
 * full of expired keys, so keep going.
 *
 * <p>That is the binomial argument Redis uses: repeating while the hit rate stays
 * above 25% keeps the expected share of stale-but-resident keys under 25%, while CPU
 * spent scales with the actual expiry rate rather than with keyspace size. A full scan
 * every tick would give the same guarantee but pay for it in latency spikes.
 */
public final class ExpirationCycle {

    private static final int SAMPLE_SIZE = 20;
    private static final double CONTINUE_THRESHOLD = 0.25;
    /** Ceiling on repeats per tick, so a pathological keyspace cannot monopolise the thread. */
    private static final int MAX_ROUNDS_PER_DB = 16;

    private final Database[] databases;
    private final long intervalMillis;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong expiredKeys = new AtomicLong();
    private final AtomicLong cycles = new AtomicLong();

    public ExpirationCycle(Database[] databases, long intervalMillis) {
        this.databases = databases;
        this.intervalMillis = intervalMillis;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "redis-expiry-cycle");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::runCycle, intervalMillis, intervalMillis,
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    /** Visible for tests: run one full cycle synchronously. */
    public int runCycle() {
        int removed = 0;
        try {
            for (Database database : databases) {
                for (int round = 0; round < MAX_ROUNDS_PER_DB; round++) {
                    Database.ExpireSample sample = database.activeExpireSample(SAMPLE_SIZE);
                    removed += sample.expired();
                    if (sample.examined() == 0
                            || (double) sample.expired() / sample.examined() <= CONTINUE_THRESHOLD) {
                        break;
                    }
                }
            }
            cycles.incrementAndGet();
            expiredKeys.addAndGet(removed);
        } catch (RuntimeException e) {
            // A background sweep must never take the scheduler down with it.
            System.err.println("[expiry] cycle failed: " + e);
        }
        return removed;
    }

    public long expiredKeys() {
        return expiredKeys.get();
    }

    public long cycles() {
        return cycles.get();
    }
}
