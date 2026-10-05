package com.rishi.redis;

import com.rishi.redis.persist.SnapshotManager;
import com.rishi.redis.server.ServerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FR-9 — a snapshot must reload into exactly the keyspace that produced it. */
class SnapshotTest {

    @TempDir
    Path directory;

    private ServerHarness harness(Path dir) {
        return new ServerHarness(ServerConfig.defaults()
                .withDir(dir)
                .withDbFilename("test.ordb")
                .withLoadSnapshotOnStart(false)
                .withSaveSnapshotOnShutdown(false));
    }

    @Test
    void savesAndReloadsEveryType() throws IOException {
        ServerHarness first = harness(directory);
        first.run("SET greeting hello");
        first.run("SET counter 42");
        first.run("RPUSH tasks a b c");
        first.run("SADD colours red green");
        first.run("HSET user name rishi role author");
        first.run("ZADD leaders 10 alice 20 bob");
        first.run("SELECT 3");
        first.run("SET other db3");
        first.run("SELECT 0");
        first.run("SET fleeting soon EX 3600");

        long written = first.context().snapshots().save();
        assertTrue(written >= 7, "expected every key to be written, got " + written);
        assertTrue(Files.exists(directory.resolve("test.ordb")));

        ServerHarness reloaded = harness(directory);
        long restored = reloaded.context().snapshots().load();
        assertEquals(written, restored);

        assertEquals("hello", reloaded.text("GET greeting"));
        assertEquals("(integer) 43", reloaded.text("INCR counter"));
        assertEquals("1) a\n2) b\n3) c", reloaded.text("LRANGE tasks 0 -1"));
        assertEquals("(integer) 2", reloaded.text("SCARD colours"));
        assertEquals("rishi", reloaded.text("HGET user name"));
        assertEquals("1) alice\n2) bob", reloaded.text("ZRANGE leaders 0 -1"));

        reloaded.run("SELECT 3");
        assertEquals("db3", reloaded.text("GET other"));
        reloaded.run("SELECT 0");

        long ttl = ((com.rishi.redis.protocol.RespValue.Integer64) reloaded.run("TTL fleeting")).value();
        assertTrue(ttl > 3500 && ttl <= 3600, "ttl should survive the round trip, was " + ttl);
    }

    @Test
    void alreadyExpiredKeysAreNotResurrected() throws IOException, InterruptedException {
        ServerHarness first = harness(directory);
        first.run("SET permanent yes");
        first.run("SET doomed no PX 40");
        first.context().snapshots().save();

        Thread.sleep(80);

        ServerHarness reloaded = harness(directory);
        long restored = reloaded.context().snapshots().load();
        assertEquals(1, restored);
        assertEquals("yes", reloaded.text("GET permanent"));
        assertEquals("(nil)", reloaded.text("GET doomed"));
    }

    @Test
    void loadingAnAbsentSnapshotIsNotAnError() throws IOException {
        SnapshotManager manager = new SnapshotManager(
                directory.resolve("never-written.ordb"), harness(directory).context().databases());
        assertEquals(0L, manager.load());
    }
}
