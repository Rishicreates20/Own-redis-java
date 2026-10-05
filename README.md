# Own-Redis-Java

A from-scratch Redis server for the JVM, plus a browser terminal for driving it.

Native Redis serialises every command on one event loop to sidestep race conditions.
This project takes the opposite bet: a lock-free, multi-threaded core built on
`java.util.concurrent`, so command execution scales across cores while the network layer
stays non-blocking.

```
repo/
├── server/     Java 21 + Netty. The actual database.       (mvn package)
└── src/        React + Vite Web CLI that talks to it.      (npm run dev)
```

## Run both halves

**1 — the server** (JDK 21+, Maven 3.9+):

```bash
cd server
mvn package
java -jar target/own-redis-java.jar
```

It listens on **6379** for RESP clients and **8080** for the Web CLI's REST bridge.

**2 — the Web CLI:**

```bash
npm install
npm run dev
```

Open the printed URL. The endpoint box in the sidebar already points at
`http://localhost:8080/api/command`; CORS is handled by the server, so nothing else
needs configuring. Type `SET mykey Hello`, then `GET mykey`.

**3 — or skip the browser entirely:**

```bash
redis-cli -p 6379
127.0.0.1:6379> set greeting "hello world"
OK
127.0.0.1:6379> get greeting
"hello world"
```

Unmodified Redis clients work because the server speaks real RESP2 — `redis-cli`,
Lettuce, Jedis and `redis-benchmark` all connect without knowing the difference.

## What's implemented

| Group | Commands |
| --- | --- |
| String | `GET SET SETNX SETEX PSETEX GETSET APPEND STRLEN INCR DECR INCRBY DECRBY INCRBYFLOAT MSET MSETNX MGET` |
| Generic | `DEL UNLINK EXISTS TYPE KEYS SCAN RANDOMKEY RENAME RENAMENX EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST` |
| Hash | `HSET HMSET HSETNX HGET HMGET HDEL HLEN HEXISTS HKEYS HVALS HGETALL HINCRBY HINCRBYFLOAT` |
| List | `LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LLEN LRANGE LINDEX LREM` |
| Set | `SADD SREM SMEMBERS SISMEMBER SCARD SPOP SRANDMEMBER SUNION SINTER SDIFF SMOVE` |
| Sorted set | `ZADD ZINCRBY ZREM ZSCORE ZCARD ZCOUNT ZRANK ZREVRANK ZRANGE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZPOPMIN ZPOPMAX` |
| Transactions | `MULTI EXEC DISCARD WATCH UNWATCH` |
| Pub/Sub | `SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBLISH PUBSUB` |
| Connection | `PING ECHO SELECT AUTH HELLO QUIT RESET CLIENT` |
| Server | `COMMAND INFO DBSIZE FLUSHDB FLUSHALL SAVE BGSAVE LASTSAVE CONFIG TIME SHUTDOWN` |

Out of scope for v1.0, per the PRD: clustering and replication, Lua scripting, AOF
durability, and RESP3 push types.

See [`server/README.md`](server/README.md) for the architecture walkthrough, the
configuration flags, and the trade-offs each structure buys.

## REST bridge

The Web CLI's contract, served on port 8080:

```
POST /api/command    {"command": "SET mykey Hello"}  ->  {"result": "OK", "error": false}
GET  /api/health                                     ->  {"status": "ok", ...}
GET  /api/messages?sessionId=<id>                    ->  queued Pub/Sub pushes
```

Replies are rendered the way `redis-cli` prints them (`(integer) 3`, `(nil)`,
`1) "first"`), so the terminal pane reads like a real session. Pass a `sessionId` to keep
`SELECT`, `MULTI` and subscriptions bound to one browser tab.
