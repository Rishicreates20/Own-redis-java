package com.rishi.redis.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.UnaryOperator;

/**
 * TD-4 — one numbered keyspace (what {@code SELECT n} picks).
 *
 * <p>The dictionary is a {@link ConcurrentHashMap}: reads never lock, writes take a
 * per-bin lock only when hashes collide, so hundreds of threads run GET/SET without
 * ever contending on a global lock. Read-modify-write commands ({@code INCR},
 * {@code APPEND}) go through {@link #updateString}, which runs the remap inside
 * {@code compute()} — the map holds the bin lock for the duration, which is what
 * makes them atomic without a lock of our own.
 *
 * <p>Values are stored as their natural JVM structure and the type is recovered by
 * {@code instanceof}:
 * <ul>
 *   <li>{@code String} — a Redis string (a wire string; see {@code Bytes})</li>
 *   <li>{@code ConcurrentLinkedDeque} — a list (lock-free at both ends)</li>
 *   <li>{@code Set} — a set ({@code ConcurrentHashMap.newKeySet()})</li>
 *   <li>{@code ConcurrentHashMap} — a hash</li>
 *   <li>{@link RedisZSet} — a sorted set</li>
 * </ul>
 *
 * <p>TTLs live in a second map of absolute epoch millis, checked lazily on every
 * access here and swept in the background by {@link ExpirationCycle}.
 */
public final class Database {

    public static final String TYPE_NONE = "none";
    public static final String TYPE_STRING = "string";
    public static final String TYPE_LIST = "list";
    public static final String TYPE_SET = "set";
    public static final String TYPE_HASH = "hash";
    public static final String TYPE_ZSET = "zset";

    /** Outcome of one active-expiry sample; drives the adaptive loop. */
    public record ExpireSample(int examined, int expired) {
    }

    private final int index;
    private final ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> expires = new ConcurrentHashMap<>();

    /**
     * TD-7 — optimistic-locking version counters. An entry only exists once a client
     * has actually WATCHed the key, so unwatched traffic pays nothing but a failed
     * hash lookup per write.
     */
    private final ConcurrentHashMap<String, AtomicLong> versions = new ConcurrentHashMap<>();

    /**
     * TD-7 — transaction isolation. Ordinary commands take the read lock (they may run
     * in parallel with each other); EXEC takes the write lock so a queued batch applies
     * with nothing interleaved.
     */
    private final ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock();

    public Database(int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    public Lock commandLock() {
        return transactionLock.readLock();
    }

    public Lock transactionLock() {
        return transactionLock.writeLock();
    }

    // ---------------------------------------------------------------- expiry

    /** Lazy expiration: drop the key if its deadline has passed. */
    private boolean expireIfNeeded(String key) {
        Long deadline = expires.get(key);
        if (deadline == null || deadline > System.currentTimeMillis()) {
            return false;
        }
        // Remove only if nobody re-armed the TTL in the meantime.
        if (expires.remove(key, deadline)) {
            data.remove(key);
            bumpVersion(key);
            return true;
        }
        return false;
    }

    public void setExpireAt(String key, long epochMillis) {
        expires.put(key, epochMillis);
        bumpVersion(key);
    }

    public Long expireAt(String key) {
        return expires.get(key);
    }

    /** @return remaining life in millis, {@code -1} when persistent, {@code -2} when absent */
    public long ttlMillis(String key) {
        if (lookup(key) == null) {
            return -2L;
        }
        Long deadline = expires.get(key);
        if (deadline == null) {
            return -1L;
        }
        return Math.max(0L, deadline - System.currentTimeMillis());
    }

    public boolean persist(String key) {
        if (lookup(key) == null) {
            return false;
        }
        boolean removed = expires.remove(key) != null;
        if (removed) {
            bumpVersion(key);
        }
        return removed;
    }

    /**
     * FR-6 — one probabilistic active-expiry sample: look at up to {@code sampleSize}
     * keys that carry a TTL and delete the dead ones. The caller loops while the hit
     * rate stays above 25%, which is what bounds stale keys without ever scanning the
     * whole keyspace.
     */
    public ExpireSample activeExpireSample(int sampleSize) {
        int total = expires.size();
        if (total == 0) {
            return new ExpireSample(0, 0);
        }
        Iterator<Map.Entry<String, Long>> iterator = expires.entrySet().iterator();
        int skip = total > sampleSize ? ThreadLocalRandom.current().nextInt(total) : 0;
        while (skip-- > 0 && iterator.hasNext()) {
            iterator.next();
        }
        long now = System.currentTimeMillis();
        int examined = 0;
        int expired = 0;
        while (iterator.hasNext() && examined < sampleSize) {
            Map.Entry<String, Long> entry = iterator.next();
            examined++;
            if (entry.getValue() <= now && expires.remove(entry.getKey(), entry.getValue())) {
                data.remove(entry.getKey());
                bumpVersion(entry.getKey());
                expired++;
            }
        }
        return new ExpireSample(examined, expired);
    }

    // ------------------------------------------------------------- versions

    /** Registers a watch slot for {@code key} and returns its current version. */
    public long version(String key) {
        return versions.computeIfAbsent(key, k -> new AtomicLong()).get();
    }

    /**
     * Marks {@code key} as modified so any WATCH on it fails at EXEC. Handlers that
     * mutate a container fetched through a read accessor (HDEL, SREM, LPOP...) must
     * call this — the read accessors deliberately do not bump.
     */
    public void touch(String key) {
        bumpVersion(key);
    }

    private void bumpVersion(String key) {
        AtomicLong counter = versions.get(key);
        if (counter != null) {
            counter.incrementAndGet();
        }
    }

    /** Invalidates every outstanding WATCH; used by FLUSHDB / FLUSHALL. */
    private void bumpAllVersions() {
        for (AtomicLong counter : versions.values()) {
            counter.incrementAndGet();
        }
    }

    // --------------------------------------------------------- generic keys

    /** @return the raw value, honouring lazy expiration; {@code null} when absent */
    public Object lookup(String key) {
        expireIfNeeded(key);
        return data.get(key);
    }

    public boolean exists(String key) {
        return lookup(key) != null;
    }

    public boolean delete(String key) {
        expireIfNeeded(key);
        boolean removed = data.remove(key) != null;
        expires.remove(key);
        if (removed) {
            bumpVersion(key);
        }
        return removed;
    }

    public void put(String key, Object value) {
        data.put(key, value);
        bumpVersion(key);
    }

    /** Live key count, excluding keys that are expired but not yet reclaimed. */
    public int size() {
        int count = 0;
        for (String key : data.keySet()) {
            if (lookup(key) != null) {
                count++;
            }
        }
        return count;
    }

    public List<String> keys(String pattern) {
        List<String> matched = new ArrayList<>();
        for (String key : data.keySet()) {
            if (GlobPattern.matches(pattern, key) && lookup(key) != null) {
                matched.add(key);
            }
        }
        return matched;
    }

    public String randomKey() {
        List<String> all = new ArrayList<>(data.keySet());
        while (!all.isEmpty()) {
            String candidate = all.remove(ThreadLocalRandom.current().nextInt(all.size()));
            if (lookup(candidate) != null) {
                return candidate;
            }
        }
        return null;
    }

    public String type(String key) {
        Object value = lookup(key);
        if (value == null) {
            return TYPE_NONE;
        }
        return typeOf(value);
    }

    public static String typeOf(Object value) {
        if (value instanceof String) {
            return TYPE_STRING;
        }
        if (value instanceof ConcurrentLinkedDeque) {
            return TYPE_LIST;
        }
        if (value instanceof RedisZSet) {
            return TYPE_ZSET;
        }
        if (value instanceof Map) {
            return TYPE_HASH;
        }
        if (value instanceof Set) {
            return TYPE_SET;
        }
        return TYPE_NONE;
    }

    public void flush() {
        data.clear();
        expires.clear();
        bumpAllVersions();
    }

    /**
     * TD-8 — the live dictionary, exposed for weakly-consistent snapshot iteration.
     * The returned view must be treated as read-only by callers other than the loader.
     */
    public ConcurrentHashMap<String, Object> rawData() {
        return data;
    }

    public ConcurrentHashMap<String, Long> rawExpires() {
        return expires;
    }

    /** Snapshot reload path: install a value (and its deadline) without touching TTL logic. */
    public void loadEntry(String key, Object value, long expireAtMillis) {
        data.put(key, value);
        if (expireAtMillis > 0) {
            expires.put(key, expireAtMillis);
        }
    }

    // ---------------------------------------------------------------- types

    public String getString(String key) {
        Object value = lookup(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw new WrongTypeException();
        }
        return text;
    }

    /** SET semantics: replaces the value and clears any TTL. */
    public void setString(String key, String value) {
        expires.remove(key);
        data.put(key, value);
        bumpVersion(key);
    }

    /** SET ... KEEPTTL / GETSET semantics: replaces the value, keeps the TTL. */
    public void setStringKeepTtl(String key, String value) {
        data.put(key, value);
        bumpVersion(key);
    }

    /**
     * Atomic read-modify-write on a string key. The remap runs under the map's own
     * bin lock, so concurrent INCRs cannot lose an update. Returning {@code null}
     * from {@code updater} deletes the key.
     */
    public String updateString(String key, UnaryOperator<String> updater) {
        expireIfNeeded(key);
        Object result = data.compute(key, (k, current) -> {
            if (current != null && !(current instanceof String)) {
                throw new WrongTypeException();
            }
            return updater.apply((String) current);
        });
        bumpVersion(key);
        return (String) result;
    }

    @SuppressWarnings("unchecked")
    public Deque<String> listForRead(String key) {
        Object value = lookup(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof ConcurrentLinkedDeque)) {
            throw new WrongTypeException();
        }
        return (Deque<String>) value;
    }

    @SuppressWarnings("unchecked")
    public Deque<String> listForWrite(String key) {
        expireIfNeeded(key);
        Object value = data.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<String>());
        if (!(value instanceof ConcurrentLinkedDeque)) {
            throw new WrongTypeException();
        }
        bumpVersion(key);
        return (Deque<String>) value;
    }

    @SuppressWarnings("unchecked")
    public Set<String> setForRead(String key) {
        Object value = lookup(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Set)) {
            throw new WrongTypeException();
        }
        return (Set<String>) value;
    }

    @SuppressWarnings("unchecked")
    public Set<String> setForWrite(String key) {
        expireIfNeeded(key);
        Object value = data.computeIfAbsent(key, k -> ConcurrentHashMap.<String>newKeySet());
        if (!(value instanceof Set)) {
            throw new WrongTypeException();
        }
        bumpVersion(key);
        return (Set<String>) value;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> hashForRead(String key) {
        Object value = lookup(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof ConcurrentHashMap)) {
            throw new WrongTypeException();
        }
        return (Map<String, String>) value;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> hashForWrite(String key) {
        expireIfNeeded(key);
        Object value = data.computeIfAbsent(key, k -> new ConcurrentHashMap<String, String>());
        if (!(value instanceof ConcurrentHashMap)) {
            throw new WrongTypeException();
        }
        bumpVersion(key);
        return (Map<String, String>) value;
    }

    public RedisZSet zsetForRead(String key) {
        Object value = lookup(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof RedisZSet zset)) {
            throw new WrongTypeException();
        }
        return zset;
    }

    public RedisZSet zsetForWrite(String key) {
        expireIfNeeded(key);
        Object value = data.computeIfAbsent(key, k -> new RedisZSet());
        if (!(value instanceof RedisZSet zset)) {
            throw new WrongTypeException();
        }
        bumpVersion(key);
        return zset;
    }

    /** Redis deletes a collection key as soon as its last element goes. */
    public void removeIfEmpty(String key, Object container) {
        boolean empty = switch (container) {
            case Collection<?> collection -> collection.isEmpty();
            case Map<?, ?> map -> map.isEmpty();
            case RedisZSet zset -> zset.isEmpty();
            default -> false;
        };
        if (empty) {
            data.remove(key, container);
            expires.remove(key);
            bumpVersion(key);
        }
    }
}
