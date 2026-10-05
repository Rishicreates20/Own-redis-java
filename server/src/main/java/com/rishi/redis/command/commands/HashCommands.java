package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * FR-4 — hashes.
 *
 * <p>Each hash is its own {@link java.util.concurrent.ConcurrentHashMap}, so two
 * clients touching different fields of the same key never contend, and HINCRBY gets
 * its atomicity from the inner map's own {@code compute()} rather than from a lock
 * over the whole key.
 */
public final class HashCommands {

    private HashCommands() {
    }

    public static void register(CommandTable table) {
        table.register("hset", -4, HashCommands::hset, Flag.WRITE);
        table.register("hmset", -4, HashCommands::hmset, Flag.WRITE);
        table.register("hsetnx", 4, HashCommands::hsetnx, Flag.WRITE);
        table.register("hget", 3, HashCommands::hget);
        table.register("hmget", -3, HashCommands::hmget);
        table.register("hdel", -3, HashCommands::hdel, Flag.WRITE);
        table.register("hlen", 2, HashCommands::hlen);
        table.register("hexists", 3, HashCommands::hexists);
        table.register("hkeys", 2, ctx -> fields(ctx, true, false));
        table.register("hvals", 2, ctx -> fields(ctx, false, true));
        table.register("hgetall", 2, ctx -> fields(ctx, true, true));
        table.register("hincrby", 4, HashCommands::hincrby, Flag.WRITE);
        table.register("hincrbyfloat", 4, HashCommands::hincrbyfloat, Flag.WRITE);
    }

    static RespValue hset(CommandContext ctx) {
        if ((ctx.argc() - 2) % 2 != 0) {
            throw RedisException.wrongArgs("hset");
        }
        Map<String, String> hash = ctx.db().hashForWrite(ctx.key());
        long added = 0;
        for (int i = 2; i < ctx.argc(); i += 2) {
            if (hash.put(ctx.arg(i), ctx.arg(i + 1)) == null) {
                added++;
            }
        }
        return RespValue.integer(added);
    }

    static RespValue hmset(CommandContext ctx) {
        hset(ctx);
        return RespValue.OK;
    }

    static RespValue hsetnx(CommandContext ctx) {
        Map<String, String> hash = ctx.db().hashForWrite(ctx.key());
        boolean added = hash.putIfAbsent(ctx.arg(2), ctx.arg(3)) == null;
        if (!added) {
            ctx.db().removeIfEmpty(ctx.key(), hash);
        }
        return RespValue.bool(added);
    }

    static RespValue hget(CommandContext ctx) {
        Map<String, String> hash = ctx.db().hashForRead(ctx.key());
        return RespValue.bulk(hash == null ? null : hash.get(ctx.arg(2)));
    }

    static RespValue hmget(CommandContext ctx) {
        Map<String, String> hash = ctx.db().hashForRead(ctx.key());
        List<RespValue> replies = new ArrayList<>(ctx.argc() - 2);
        for (int i = 2; i < ctx.argc(); i++) {
            replies.add(RespValue.bulk(hash == null ? null : hash.get(ctx.arg(i))));
        }
        return RespValue.array(replies);
    }

    static RespValue hdel(CommandContext ctx) {
        Map<String, String> hash = ctx.db().hashForRead(ctx.key());
        if (hash == null) {
            return RespValue.ZERO;
        }
        long removed = 0;
        for (int i = 2; i < ctx.argc(); i++) {
            if (hash.remove(ctx.arg(i)) != null) {
                removed++;
            }
        }
        if (removed > 0) {
            ctx.db().touch(ctx.key());
        }
        ctx.db().removeIfEmpty(ctx.key(), hash);
        return RespValue.integer(removed);
    }

    static RespValue hlen(CommandContext ctx) {
        Map<String, String> hash = ctx.db().hashForRead(ctx.key());
        return RespValue.integer(hash == null ? 0 : hash.size());
    }

    static RespValue hexists(CommandContext ctx) {
        Map<String, String> hash = ctx.db().hashForRead(ctx.key());
        return RespValue.bool(hash != null && hash.containsKey(ctx.arg(2)));
    }

    /** Backs HKEYS, HVALS and HGETALL — the same walk with different projections. */
    static RespValue fields(CommandContext ctx, boolean includeFields, boolean includeValues) {
        Map<String, String> hash = ctx.db().hashForRead(ctx.key());
        if (hash == null) {
            return RespValue.emptyArray();
        }
        List<RespValue> out = new ArrayList<>(hash.size() * 2);
        for (Map.Entry<String, String> entry : hash.entrySet()) {
            if (includeFields) {
                out.add(RespValue.bulk(entry.getKey()));
            }
            if (includeValues) {
                out.add(RespValue.bulk(entry.getValue()));
            }
        }
        return RespValue.array(out);
    }

    static RespValue hincrby(CommandContext ctx) {
        long delta = ctx.longArg(3);
        Map<String, String> hash = ctx.db().hashForWrite(ctx.key());
        String updated = hash.compute(ctx.arg(2), (field, current) -> {
            long base;
            try {
                base = current == null ? 0L : Long.parseLong(current);
            } catch (NumberFormatException e) {
                throw new RedisException("ERR hash value is not an integer");
            }
            long next = base + delta;
            if (((base ^ next) & (delta ^ next)) < 0) {
                throw new RedisException("ERR increment or decrement would overflow");
            }
            return Long.toString(next);
        });
        return RespValue.integer(Long.parseLong(updated));
    }

    static RespValue hincrbyfloat(CommandContext ctx) {
        double delta = ctx.doubleArg(3);
        Map<String, String> hash = ctx.db().hashForWrite(ctx.key());
        String updated = hash.compute(ctx.arg(2), (field, current) -> {
            double base;
            try {
                base = current == null ? 0.0 : Double.parseDouble(current);
            } catch (NumberFormatException e) {
                throw new RedisException("ERR hash value is not a float");
            }
            double next = base + delta;
            if (Double.isNaN(next) || Double.isInfinite(next)) {
                throw new RedisException("ERR increment would produce NaN or Infinity");
            }
            return RespValue.formatDouble(next);
        });
        return RespValue.bulk(updated);
    }
}
