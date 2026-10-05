package com.rishi.redis.persist;

import com.rishi.redis.protocol.Bytes;
import com.rishi.redis.store.Database;
import com.rishi.redis.store.RedisZSet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TD-8 / FR-9 — RDB-style point-in-time-ish snapshots.
 *
 * <p>Native Redis forks and lets the OS give the child a copy-on-write view of memory,
 * so its dump is a true instant. The JVM has no safe {@code fork()}, so this takes the
 * other option available to it: the writer walks the {@link java.util.concurrent.ConcurrentHashMap}
 * with a <em>weakly consistent</em> iterator. Such an iterator never throws
 * {@code ConcurrentModificationException}, never returns an element twice, and reflects
 * some — not necessarily all — concurrent updates. The result is not a microsecond-exact
 * instant, but it is lock-free: no writer is ever blocked by a save in progress.
 *
 * <p>The file is written to a sibling temp file and atomically moved into place, so an
 * interrupted save can never truncate the previous good snapshot.
 *
 * <p>Format (all integers big-endian, strings as {@code int length + ISO-8859-1 bytes}):
 * <pre>
 *   "ORDB" | int version | long savedAt | int dbCount
 *   per db:    int index | int entryCount
 *   per entry: byte type | string key | long expireAt (-1 = none) | payload
 * </pre>
 */
public final class SnapshotManager {

    private static final byte[] MAGIC = {'O', 'R', 'D', 'B'};
    private static final int FORMAT_VERSION = 1;

    private static final byte TYPE_STRING = 0;
    private static final byte TYPE_LIST = 1;
    private static final byte TYPE_SET = 2;
    private static final byte TYPE_HASH = 3;
    private static final byte TYPE_ZSET = 4;

    private final Path path;
    private final Database[] databases;
    private final AtomicBoolean saving = new AtomicBoolean();
    private final AtomicLong lastSaveTime = new AtomicLong();
    private final AtomicLong changesSinceSave = new AtomicLong();

    public SnapshotManager(Path path, Database[] databases) {
        this.path = path;
        this.databases = databases;
    }

    public Path path() {
        return path;
    }

    public boolean isSaving() {
        return saving.get();
    }

    /** Epoch seconds of the last successful save, as LASTSAVE reports it. */
    public long lastSaveTimeSeconds() {
        return lastSaveTime.get() / 1000L;
    }

    public long changesSinceSave() {
        return changesSinceSave.get();
    }

    public void recordChange() {
        changesSinceSave.incrementAndGet();
    }

    /**
     * SAVE — writes the snapshot on the calling thread.
     *
     * @return number of keys written
     */
    public long save() throws IOException {
        if (!saving.compareAndSet(false, true)) {
            throw new IOException("Background save already in progress");
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path temp = path.resolveSibling(path.getFileName() + ".tmp-" + ProcessHandle.current().pid());
            long written;
            try (FileChannel channel = FileChannel.open(temp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
                 DataOutputStream out = new DataOutputStream(
                         new BufferedOutputStream(Channels.newOutputStream(channel), 1 << 16))) {
                written = writeAll(out);
                out.flush();
                channel.force(true);
            }
            moveIntoPlace(temp);
            lastSaveTime.set(System.currentTimeMillis());
            changesSinceSave.set(0L);
            return written;
        } finally {
            saving.set(false);
        }
    }

    /**
     * BGSAVE — same work on a background thread, so the caller's connection is not
     * held while the dump is written.
     *
     * @return false when a save is already running
     */
    public boolean backgroundSave() {
        if (saving.get()) {
            return false;
        }
        Thread worker = new Thread(() -> {
            try {
                long written = save();
                System.out.println("[persist] background save finished: " + written + " keys -> " + path);
            } catch (IOException e) {
                System.err.println("[persist] background save failed: " + e.getMessage());
            }
        }, "redis-bgsave");
        worker.setDaemon(true);
        worker.start();
        return true;
    }

    private void moveIntoPlace(Path temp) throws IOException {
        try {
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private long writeAll(DataOutputStream out) throws IOException {
        out.write(MAGIC);
        out.writeInt(FORMAT_VERSION);
        out.writeLong(System.currentTimeMillis());
        out.writeInt(databases.length);

        long total = 0;
        for (Database database : databases) {
            // Snapshot the key list first so the entry count matches what we write,
            // even though the underlying map may keep changing beneath us.
            List<String> keys = new ArrayList<>(database.rawData().keySet());
            List<String> live = new ArrayList<>(keys.size());
            for (String key : keys) {
                if (database.lookup(key) != null) {
                    live.add(key);
                }
            }
            out.writeInt(database.index());
            out.writeInt(live.size());
            for (String key : live) {
                Object value = database.rawData().get(key);
                if (value == null) {
                    // Vanished mid-walk: write a tombstone-ish empty string so the
                    // declared count still matches the payload count.
                    writeEntry(out, key, "", -1L);
                    continue;
                }
                Long expireAt = database.expireAt(key);
                writeEntry(out, key, value, expireAt == null ? -1L : expireAt);
                total++;
            }
        }
        return total;
    }

    @SuppressWarnings("unchecked")
    private void writeEntry(DataOutputStream out, String key, Object value, long expireAt)
            throws IOException {
        switch (value) {
            case String text -> {
                out.writeByte(TYPE_STRING);
                writeString(out, key);
                out.writeLong(expireAt);
                writeString(out, text);
            }
            case ConcurrentLinkedDeque<?> list -> {
                List<String> copy = new ArrayList<>((Deque<String>) list);
                out.writeByte(TYPE_LIST);
                writeString(out, key);
                out.writeLong(expireAt);
                out.writeInt(copy.size());
                for (String element : copy) {
                    writeString(out, element);
                }
            }
            case RedisZSet zset -> {
                List<RedisZSet.ScoredMember> members = zset.all();
                out.writeByte(TYPE_ZSET);
                writeString(out, key);
                out.writeLong(expireAt);
                out.writeInt(members.size());
                for (RedisZSet.ScoredMember member : members) {
                    writeString(out, member.member());
                    out.writeDouble(member.score());
                }
            }
            case Map<?, ?> hash -> {
                Map<String, String> copy = new java.util.LinkedHashMap<>((Map<String, String>) hash);
                out.writeByte(TYPE_HASH);
                writeString(out, key);
                out.writeLong(expireAt);
                out.writeInt(copy.size());
                for (Map.Entry<String, String> field : copy.entrySet()) {
                    writeString(out, field.getKey());
                    writeString(out, field.getValue());
                }
            }
            case Set<?> set -> {
                List<String> copy = new ArrayList<>((Set<String>) set);
                out.writeByte(TYPE_SET);
                writeString(out, key);
                out.writeLong(expireAt);
                out.writeInt(copy.size());
                for (String member : copy) {
                    writeString(out, member);
                }
            }
            default -> {
                out.writeByte(TYPE_STRING);
                writeString(out, key);
                out.writeLong(expireAt);
                writeString(out, String.valueOf(value));
            }
        }
    }

    /**
     * Reloads the snapshot at boot. Keys whose deadline already passed are dropped
     * rather than resurrected.
     *
     * @return number of keys restored, or 0 when there is no snapshot
     */
    public long load() throws IOException {
        if (!Files.exists(path)) {
            return 0L;
        }
        long restored = 0;
        try (InputStream file = Files.newInputStream(path, StandardOpenOption.READ);
             DataInputStream in = new DataInputStream(new BufferedInputStream(file, 1 << 16))) {

            byte[] magic = in.readNBytes(4);
            if (magic.length != 4 || magic[0] != MAGIC[0] || magic[1] != MAGIC[1]
                    || magic[2] != MAGIC[2] || magic[3] != MAGIC[3]) {
                throw new IOException("not an Own-Redis snapshot: " + path);
            }
            int version = in.readInt();
            if (version != FORMAT_VERSION) {
                throw new IOException("unsupported snapshot version " + version);
            }
            in.readLong(); // savedAt, informational
            int dbCount = in.readInt();
            long now = System.currentTimeMillis();

            for (int d = 0; d < dbCount; d++) {
                int index = in.readInt();
                int entries = in.readInt();
                for (int e = 0; e < entries; e++) {
                    byte type = in.readByte();
                    String key = readString(in);
                    long expireAt = in.readLong();
                    Object value = readPayload(in, type);
                    boolean alive = expireAt < 0 || expireAt > now;
                    if (alive && index >= 0 && index < databases.length) {
                        databases[index].loadEntry(key, value, expireAt);
                        restored++;
                    }
                }
            }
        } catch (EOFException e) {
            throw new IOException("snapshot is truncated: " + path, e);
        }
        lastSaveTime.set(System.currentTimeMillis());
        return restored;
    }

    private Object readPayload(DataInputStream in, byte type) throws IOException {
        switch (type) {
            case TYPE_STRING -> {
                return readString(in);
            }
            case TYPE_LIST -> {
                int count = in.readInt();
                ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                for (int i = 0; i < count; i++) {
                    list.addLast(readString(in));
                }
                return list;
            }
            case TYPE_SET -> {
                int count = in.readInt();
                Set<String> set = ConcurrentHashMap.newKeySet();
                for (int i = 0; i < count; i++) {
                    set.add(readString(in));
                }
                return set;
            }
            case TYPE_HASH -> {
                int count = in.readInt();
                ConcurrentHashMap<String, String> hash = new ConcurrentHashMap<>();
                for (int i = 0; i < count; i++) {
                    String field = readString(in);
                    hash.put(field, readString(in));
                }
                return hash;
            }
            case TYPE_ZSET -> {
                int count = in.readInt();
                RedisZSet zset = new RedisZSet();
                for (int i = 0; i < count; i++) {
                    String member = readString(in);
                    zset.add(member, in.readDouble());
                }
                return zset;
            }
            default -> throw new IOException("unknown value type in snapshot: " + type);
        }
    }

    private static void writeString(DataOutputStream out, String wire) throws IOException {
        byte[] raw = Bytes.toBytes(wire);
        out.writeInt(raw.length);
        out.write(raw);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > 512 * 1024 * 1024) {
            throw new IOException("corrupt snapshot: bad string length " + length);
        }
        byte[] raw = in.readNBytes(length);
        if (raw.length != length) {
            throw new EOFException("corrupt snapshot: short read");
        }
        return Bytes.fromBytes(raw);
    }
}
