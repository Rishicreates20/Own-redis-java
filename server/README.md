# Own-Redis-Java — server

A RESP-compatible, multi-threaded in-memory key–value store on the JVM. Implements the
v1.0 surface from the PRD: strings, hashes, lists, sets, sorted sets, TTLs with lazy +
active expiration, MULTI/EXEC/WATCH, Pub/Sub, and RDB-style snapshots.

Two front doors onto one server:

| Port | Protocol | For |
| --- | --- | --- |
| 6379 | RESP2 over TCP | `redis-cli`, Lettuce, Jedis, `redis-benchmark` |
| 8080 | HTTP + JSON | the React Web CLI in this repo |

## Build & run

```bash
cd server
mvn package
java -jar target/own-redis-java.jar
```

Requires JDK 21+ and Maven 3.9+. The build produces a single runnable fat-jar; Netty is
the only third-party dependency.

```bash
# talk to it with the real client
redis-cli -p 6379 ping
redis-cli -p 6379 set greeting "hello world"
redis-cli -p 6379 get greeting

# or from the Web CLI's REST bridge
curl -X POST http://localhost:8080/api/command \
     -H 'Content-Type: application/json' \
     -d '{"command":"SET mykey Hello"}'
# -> {"result":"OK","error":false,...}
```

## Options

```
--config <file>          load a redis.conf-style file first
--host <addr>            bind address                      (default 0.0.0.0)
--port <n>               RESP port                         (default 6379)
--http-port <n>          REST bridge port                  (default 8080)
--no-http                disable the REST bridge
--cors-origin <origin>   Access-Control-Allow-Origin       (default *)
--databases <n>          number of SELECTable databases    (default 16)
--requirepass <secret>   require AUTH before commands
--dir <path>             snapshot directory                (default .)
--dbfilename <name>      snapshot file name                (default dump.ordb)
--no-load                do not load the snapshot at boot
--no-save-on-shutdown    do not snapshot on clean shutdown
--expiry-interval <ms>   active expiry cycle period        (default 100)
--exec-model <mode>      inline | vthread                  (default inline)
```

## Architecture

The path a command takes, bottom to top:

1. **Transport** (`server/`) — Netty NIO reactor. A small event-loop pool multiplexes
   every connection; sockets are never one-thread-each.
2. **Protocol** (`protocol/`) — `RespDecoder` accumulates bytes until a complete RESP
   frame exists, so TCP fragmentation and pipelining are both handled. Lengths are
   bounded and CRLF placement is checked strictly.
3. **Dispatch** (`command/`) — an O(1) table maps the first argument to a handler.
   `CommandDispatcher` owns arity, auth, MULTI queuing, subscriber-mode rules, locking
   and error translation, so handlers hold only semantics.
4. **Store** (`store/`) — a `ConcurrentHashMap` keyspace per database. Read-modify-write
   commands run inside `compute()`, which is what makes `INCR` atomic without a global
   lock. Sorted sets pair a hash map with a `ConcurrentSkipListMap`.
5. **Expiration** (`store/ExpirationCycle`) — lazy deletion on access plus a background
   cycle that samples 20 TTL keys and repeats while more than 25% of a sample is dead.
6. **Transactions** (`command/commands/TransactionCommands`) — a per-database
   `ReentrantReadWriteLock`: ordinary commands take the read lock, `EXEC` the write lock.
   `WATCH` is optimistic, comparing per-key version counters.
7. **Persistence** (`persist/`) — snapshots written from a weakly-consistent iterator
   (the JVM has no `fork()` + copy-on-write), staged through a temp file and moved
   atomically into place.

### Known trade-offs

- **`LLEN`/`LRANGE` are O(n).** `ConcurrentLinkedDeque` gives lock-free pushes and pops
  at both ends, which is what list workloads actually hammer, but it has no index.
- **Snapshots are not a true instant.** A weakly-consistent iterator reflects some
  concurrent writes; in exchange, no writer ever blocks on a save.
- **`EXEC` locks a whole database,** not a stripe of it. Lock striping is the documented
  next step (TD-7).
- **`SCAN` cursors** are positional rather than Redis's reverse-binary cursor, so keys
  added mid-scan carry weaker guarantees.
- **Values are stored as ISO-8859-1 "wire strings".** That mapping is byte-exact in both
  directions, so binary safety holds while `String`'s cached hash and fast equality are
  still available.

## Tests

```bash
mvn test
```

Covers the parser under fragmentation, pipelining and malformed input; command semantics
per type; TTL behaviour on both the lazy and active paths; transaction isolation and
WATCH aborts; snapshot round-trips; the HTTP bridge contract; and the PRD's core
reliability claim — 16 threads × 1000 `INCR`s landing on exactly 16000.
