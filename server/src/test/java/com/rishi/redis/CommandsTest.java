package com.rishi.redis;

import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.server.ClientSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FR-3 to FR-8 — command semantics, exercised through the real dispatcher. */
class CommandsTest {

    private ServerHarness redis;

    @BeforeEach
    void setUp() {
        redis = new ServerHarness();
    }

    @Test
    void stringRoundTrip() {
        assertEquals("OK", redis.text("SET mykey Hello"));
        assertEquals("Hello", redis.text("GET mykey"));
        assertEquals("(nil)", redis.text("GET missing"));
        assertEquals("(integer) 1", redis.text("DEL mykey"));
        assertEquals("(integer) 0", redis.text("EXISTS mykey"));
    }

    @Test
    void setOptionsHonourNxXxAndTtl() {
        assertEquals("OK", redis.text("SET k v1 NX"));
        assertEquals("(nil)", redis.text("SET k v2 NX"));
        assertEquals("v1", redis.text("GET k"));
        assertEquals("OK", redis.text("SET k v3 XX"));
        assertEquals("v3", redis.text("GET k"));
        assertEquals("(nil)", redis.text("SET absent v XX"));

        assertEquals("OK", redis.text("SET k v4 EX 100"));
        long ttl = ((RespValue.Integer64) redis.run("TTL k")).value();
        assertTrue(ttl > 90 && ttl <= 100, "ttl should be about 100s but was " + ttl);
    }

    @Test
    void counterCommands() {
        assertEquals("(integer) 1", redis.text("INCR hits"));
        assertEquals("(integer) 11", redis.text("INCRBY hits 10"));
        assertEquals("(integer) 10", redis.text("DECR hits"));
        assertEquals("(integer) 5", redis.text("DECRBY hits 5"));
        assertEquals("10.5", redis.text("INCRBYFLOAT hits 5.5"));

        redis.run("SET word hello");
        assertEquals("(integer) 11", redis.text("APPEND word \" world\""));
        assertEquals("hello world", redis.text("GET word"));
        assertEquals("(error) ERR value is not an integer or out of range", redis.text("INCR word"));
    }

    @Test
    void wrongTypeIsRejected() {
        redis.run("LPUSH mylist a");
        assertEquals("(error) WRONGTYPE Operation against a key holding the wrong kind of value",
                redis.text("GET mylist"));
        assertEquals("(error) WRONGTYPE Operation against a key holding the wrong kind of value",
                redis.text("INCR mylist"));
    }

    @Test
    void listCommands() {
        assertEquals("(integer) 2", redis.text("RPUSH tasks a b"));
        assertEquals("(integer) 3", redis.text("LPUSH tasks first"));
        assertEquals("(integer) 3", redis.text("LLEN tasks"));
        assertEquals("1) first\n2) a\n3) b", redis.text("LRANGE tasks 0 -1"));
        assertEquals("first", redis.text("LPOP tasks"));
        assertEquals("b", redis.text("RPOP tasks"));
        assertEquals("(integer) 1", redis.text("LLEN tasks"));
        redis.run("LPOP tasks");
        // The key disappears with its last element, as in Redis.
        assertEquals("none", redis.text("TYPE tasks"));
    }

    @Test
    void hashCommands() {
        assertEquals("(integer) 2", redis.text("HSET user name rishi role author"));
        assertEquals("rishi", redis.text("HGET user name"));
        assertEquals("(integer) 0", redis.text("HSET user name rishikesh"));
        assertEquals("(integer) 2", redis.text("HLEN user"));
        assertEquals("(integer) 1", redis.text("HDEL user role"));
        assertEquals("(integer) 1", redis.text("HEXISTS user name"));
        assertEquals("(integer) 7", redis.text("HINCRBY user visits 7"));
    }

    @Test
    void setCommands() {
        assertEquals("(integer) 3", redis.text("SADD colours red green blue"));
        assertEquals("(integer) 0", redis.text("SADD colours red"));
        assertEquals("(integer) 3", redis.text("SCARD colours"));
        assertEquals("(integer) 1", redis.text("SISMEMBER colours red"));
        assertEquals("(integer) 1", redis.text("SREM colours red"));
        redis.run("SADD warm red orange");
        // {green, blue} union {red, orange}
        assertEquals(4, ((RespValue.Array) redis.run("SUNION colours warm")).items().size());
        assertEquals("(empty array)", redis.text("SINTER colours warm"));
    }

    @Test
    void sortedSetOrdersByScoreThenMember() {
        assertEquals("(integer) 3", redis.text("ZADD leaders 10 alice 30 carol 20 bob"));
        assertEquals("1) alice\n2) bob\n3) carol", redis.text("ZRANGE leaders 0 -1"));
        assertEquals("1) carol\n2) bob\n3) alice", redis.text("ZREVRANGE leaders 0 -1"));
        assertEquals("1) bob\n2) 20", redis.text("ZRANGEBYSCORE leaders 15 25 WITHSCORES"));
        assertEquals("20", redis.text("ZSCORE leaders bob"));
        assertEquals("(integer) 1", redis.text("ZRANK leaders bob"));
        assertEquals("(integer) 2", redis.text("ZCOUNT leaders 20 40"));
        assertEquals("1) alice\n2) 10", redis.text("ZPOPMIN leaders"));
        assertEquals("(integer) 2", redis.text("ZCARD leaders"));
    }

    @Test
    void expirationIsLazyAndActive() throws InterruptedException {
        redis.run("SET fleeting value PX 40");
        assertEquals("value", redis.text("GET fleeting"));

        Thread.sleep(80);

        // Lazy path: the read itself notices the deadline passed.
        assertEquals("(nil)", redis.text("GET fleeting"));
        assertEquals("(integer) -2", redis.text("TTL fleeting"));

        // Active path: a key nobody touches must still be reclaimed.
        redis.run("SET untouched value PX 30");
        Thread.sleep(60);
        redis.context().expiration().runCycle();
        assertEquals(0, redis.context().database(0).rawData().size());
    }

    @Test
    void persistClearsTheTtl() {
        redis.run("SET k v EX 100");
        assertEquals("(integer) 1", redis.text("PERSIST k"));
        assertEquals("(integer) -1", redis.text("TTL k"));
        assertEquals("(integer) 0", redis.text("PERSIST k"));
    }

    @Test
    void keysMatchesGlobPatterns() {
        redis.run("MSET one 1 two 2 three 3");
        assertEquals("1) one\n2) three\n3) two", redis.text("KEYS *"));
        assertEquals("1) three\n2) two", redis.text("KEYS t*"));
        assertEquals("1) one", redis.text("KEYS ?ne"));
    }

    @Test
    void transactionsQueueThenApplyAtomically() {
        assertEquals("OK", redis.text("MULTI"));
        assertEquals("QUEUED", redis.text("SET a 1"));
        assertEquals("QUEUED", redis.text("INCR a"));
        assertEquals("1) OK\n2) (integer) 2", redis.text("EXEC"));
        assertEquals("2", redis.text("GET a"));
    }

    @Test
    void discardThrowsTheBatchAway() {
        redis.run("SET a original");
        redis.run("MULTI");
        redis.run("SET a changed");
        assertEquals("OK", redis.text("DISCARD"));
        assertEquals("original", redis.text("GET a"));
        assertEquals("(error) ERR EXEC without MULTI", redis.text("EXEC"));
    }

    @Test
    void watchAbortsWhenAnotherClientTouchesTheKey() {
        ClientSession other = redis.newSession("other");
        redis.run("SET balance 100");

        redis.run("WATCH balance");
        redis.run("MULTI");
        redis.run("SET balance 50");

        // The other connection moves the watched key out from under the transaction.
        redis.run(other, "SET balance 999");

        assertEquals("(nil)", redis.text("EXEC"));
        assertEquals("999", redis.text("GET balance"));
    }

    @Test
    void watchCommitsWhenNothingChanged() {
        redis.run("SET balance 100");
        redis.run("WATCH balance");
        redis.run("MULTI");
        redis.run("SET balance 50");
        assertEquals("1) OK", redis.text("EXEC"));
        assertEquals("50", redis.text("GET balance"));
    }

    @Test
    void queuingASyntacticallyInvalidCommandAbortsTheTransaction() {
        redis.run("MULTI");
        assertTrue(redis.text("NOSUCHCOMMAND x").startsWith("(error) ERR unknown command"));
        assertEquals("(error) EXECABORT Transaction discarded because of previous errors.",
                redis.text("EXEC"));
    }

    @Test
    void pubSubDeliversToChannelAndPatternSubscribers() {
        ServerHarness subscriber = redis; // this session subscribes
        subscriber.run("SUBSCRIBE news");
        subscriber.run("PSUBSCRIBE ne*");

        ClientSession publisher = redis.newSession("publisher");
        // One channel subscription plus one matching pattern subscription.
        assertEquals("(integer) 2", toText(redis.run(publisher, "PUBLISH news hello")));

        assertTrue(subscriber.pushed().size() >= 4, "expected confirmations plus two messages");
        RespValue last = subscriber.pushed().get(subscriber.pushed().size() - 1);
        assertTrue(toText(last).contains("hello"));
    }

    @Test
    void subscriberModeRefusesOrdinaryCommands() {
        redis.run("SUBSCRIBE news");
        assertTrue(redis.text("GET anything").startsWith("(error) ERR Can't execute 'get'"));
        assertEquals("PONG", redis.text("PING"));
    }

    @Test
    void selectSwitchesDatabases() {
        redis.run("SET shared db0");
        assertEquals("OK", redis.text("SELECT 1"));
        assertEquals("(nil)", redis.text("GET shared"));
        redis.run("SET shared db1");
        redis.run("SELECT 0");
        assertEquals("db0", redis.text("GET shared"));
        assertEquals("(error) ERR DB index is out of range", redis.text("SELECT 99"));
    }

    @Test
    void arityErrorsAreReportedNotThrown() {
        assertEquals("(error) ERR wrong number of arguments for 'get' command", redis.text("GET"));
        assertEquals("(error) ERR wrong number of arguments for 'set' command", redis.text("SET onlykey"));
    }

    /**
     * The PRD's core reliability requirement: no lost updates under concurrent INCR.
     * Sixteen threads, a thousand increments each, must land on exactly 16000.
     */
    @Test
    void concurrentIncrementsNeverLoseAnUpdate() throws InterruptedException {
        int threads = 16;
        int perThread = 1_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            ClientSession session = redis.newSession("worker-" + t);
            pool.execute(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        redis.run(session, "INCR counter");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "workers did not finish in time");
        pool.shutdownNow();

        assertEquals(Long.toString((long) threads * perThread), redis.text("GET counter"));
    }

    @Test
    void concurrentPushesAllLand() throws InterruptedException {
        int threads = 8;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            ClientSession session = redis.newSession("pusher-" + t);
            pool.execute(() -> {
                for (int i = 0; i < perThread; i++) {
                    redis.run(session, "RPUSH queue item");
                }
                done.countDown();
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdownNow();
        assertEquals("(integer) " + (threads * perThread), redis.text("LLEN queue"));
    }

    @Test
    void unknownCommandsAreNamedInTheError() {
        String reply = redis.text("FLUXCAPACITOR on now");
        assertTrue(reply.startsWith("(error) ERR unknown command 'FLUXCAPACITOR'"), reply);
        assertNotEquals("", reply);
    }

    private static String toText(RespValue value) {
        return com.rishi.redis.server.ReplyFormatter.toText(value);
    }
}
