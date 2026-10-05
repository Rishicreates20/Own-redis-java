package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.store.Database;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Key-space commands that are not tied to one value type: existence, deletion,
 * TTL control, enumeration and renaming.
 */
public final class GenericCommands {

    private GenericCommands() {
    }

    public static void register(CommandTable table) {
        table.register("del", -2, GenericCommands::del, Flag.WRITE);
        table.register("unlink", -2, GenericCommands::del, Flag.WRITE);
        table.register("exists", -2, GenericCommands::exists);
        table.register("type", 2, GenericCommands::type);
        table.register("keys", 2, GenericCommands::keys);
        table.register("scan", -2, GenericCommands::scan);
        table.register("randomkey", 1, GenericCommands::randomKey);
        table.register("rename", 3, GenericCommands::rename, Flag.WRITE);
        table.register("renamenx", 3, GenericCommands::renamenx, Flag.WRITE);
        table.register("expire", -3, ctx -> expire(ctx, 1000L, false), Flag.WRITE);
        table.register("pexpire", -3, ctx -> expire(ctx, 1L, false), Flag.WRITE);
        table.register("expireat", -3, ctx -> expire(ctx, 1000L, true), Flag.WRITE);
        table.register("pexpireat", -3, ctx -> expire(ctx, 1L, true), Flag.WRITE);
        table.register("ttl", 2, ctx -> ttl(ctx, 1000L));
        table.register("pttl", 2, ctx -> ttl(ctx, 1L));
        table.register("persist", 2, GenericCommands::persist, Flag.WRITE);
    }

    static RespValue del(CommandContext ctx) {
        long deleted = 0;
        for (int i = 1; i < ctx.argc(); i++) {
            if (ctx.db().delete(ctx.arg(i))) {
                deleted++;
            }
        }
        return RespValue.integer(deleted);
    }

    static RespValue exists(CommandContext ctx) {
        long found = 0;
        for (int i = 1; i < ctx.argc(); i++) {
            if (ctx.db().exists(ctx.arg(i))) {
                found++;
            }
        }
        return RespValue.integer(found);
    }

    static RespValue type(CommandContext ctx) {
        return RespValue.simple(ctx.db().type(ctx.key()));
    }

    static RespValue keys(CommandContext ctx) {
        List<String> matched = ctx.db().keys(ctx.arg(1));
        Collections.sort(matched);
        return RespValue.bulkArray(matched);
    }

    /**
     * A simplified SCAN: the cursor is an offset into the key list ordered by hash
     * iteration, so a full pass visits every key that survives the whole scan. It does
     * not carry Redis's guarantee about keys added mid-scan — enough for interactive
     * use, and it avoids KEYS blocking a large keyspace.
     */
    static RespValue scan(CommandContext ctx) {
        long cursor = ctx.longArg(1);
        String pattern = null;
        int count = 10;
        for (int i = 2; i < ctx.argc(); i++) {
            switch (ctx.option(i)) {
                case "MATCH" -> {
                    if (++i >= ctx.argc()) {
                        throw RedisException.syntaxError();
                    }
                    pattern = ctx.arg(i);
                }
                case "COUNT" -> {
                    if (++i >= ctx.argc()) {
                        throw RedisException.syntaxError();
                    }
                    count = (int) Math.min(10_000L, Math.max(1L, ctx.longArg(i)));
                }
                case "TYPE" -> {
                    if (++i >= ctx.argc()) {
                        throw RedisException.syntaxError();
                    }
                    // Accepted and ignored: every type is scannable here.
                }
                default -> throw RedisException.syntaxError();
            }
        }

        List<String> all = new ArrayList<>(ctx.db().rawData().keySet());
        List<RespValue> page = new ArrayList<>();
        int index = (int) Math.max(0, Math.min(cursor, all.size()));
        int taken = 0;
        while (index < all.size() && taken < count) {
            String key = all.get(index++);
            taken++;
            if (ctx.db().lookup(key) == null) {
                continue;
            }
            if (pattern == null || com.rishi.redis.store.GlobPattern.matches(pattern, key)) {
                page.add(RespValue.bulk(key));
            }
        }
        long next = index >= all.size() ? 0L : index;
        return RespValue.array(List.of(RespValue.bulk(Long.toString(next)), RespValue.array(page)));
    }

    static RespValue randomKey(CommandContext ctx) {
        return RespValue.bulk(ctx.db().randomKey());
    }

    static RespValue rename(CommandContext ctx) {
        moveKey(ctx.db(), ctx.key(), ctx.arg(2));
        return RespValue.OK;
    }

    static RespValue renamenx(CommandContext ctx) {
        if (ctx.db().exists(ctx.arg(2))) {
            return RespValue.ZERO;
        }
        moveKey(ctx.db(), ctx.key(), ctx.arg(2));
        return RespValue.ONE;
    }

    private static void moveKey(Database db, String from, String to) {
        Object value = db.lookup(from);
        if (value == null) {
            throw new RedisException("ERR no such key");
        }
        Long deadline = db.expireAt(from);
        db.delete(from);
        db.put(to, value);
        if (deadline != null) {
            db.setExpireAt(to, deadline);
        } else {
            db.persist(to);
        }
    }

    /**
     * FR-6 — arms a TTL. {@code unitMillis} scales the argument (seconds vs millis) and
     * {@code absolute} selects EXPIREAT-style semantics. NX/XX/GT/LT conditions are
     * supported as in Redis 7.
     */
    static RespValue expire(CommandContext ctx, long unitMillis, boolean absolute) {
        String key = ctx.key();
        long amount = ctx.longArg(2);
        boolean nx = false;
        boolean xx = false;
        boolean gt = false;
        boolean lt = false;
        for (int i = 3; i < ctx.argc(); i++) {
            switch (ctx.option(i)) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GT" -> gt = true;
                case "LT" -> lt = true;
                default -> throw RedisException.syntaxError();
            }
        }
        if ((gt && lt) || (nx && (xx || gt || lt))) {
            throw new RedisException("ERR NX and XX, GT or LT options at the same time are not compatible");
        }
        if (!ctx.db().exists(key)) {
            return RespValue.ZERO;
        }

        long deadline = absolute
                ? amount * unitMillis
                : System.currentTimeMillis() + amount * unitMillis;
        Long current = ctx.db().expireAt(key);

        if (nx && current != null) {
            return RespValue.ZERO;
        }
        // XX and GT both need an existing TTL to compare against; LT treats a missing
        // TTL as infinity, so it always wins.
        if ((xx || gt) && current == null) {
            return RespValue.ZERO;
        }
        if (gt && current != null && deadline <= current) {
            return RespValue.ZERO;
        }
        if (lt && current != null && deadline >= current) {
            return RespValue.ZERO;
        }

        if (deadline <= System.currentTimeMillis()) {
            // A deadline in the past deletes the key immediately, as Redis does.
            ctx.db().delete(key);
            return RespValue.ONE;
        }
        ctx.db().setExpireAt(key, deadline);
        return RespValue.ONE;
    }

    static RespValue ttl(CommandContext ctx, long unitMillis) {
        long remaining = ctx.db().ttlMillis(ctx.key());
        if (remaining < 0) {
            return RespValue.integer(remaining);
        }
        if (unitMillis == 1L) {
            return RespValue.integer(remaining);
        }
        // Round up, so a key with 900ms left reports 1s rather than 0s.
        return RespValue.integer((remaining + unitMillis - 1) / unitMillis);
    }

    static RespValue persist(CommandContext ctx) {
        return RespValue.bool(ctx.db().persist(ctx.key()));
    }
}
