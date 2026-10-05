package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.store.Database;
import com.rishi.redis.store.WrongTypeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FR-3 — string commands.
 *
 * <p>The read-modify-write family (INCR, DECR, APPEND, SETRANGE-style updates) never
 * does get-then-put: every one of them goes through {@link Database#updateString},
 * which performs the whole transform inside {@code ConcurrentHashMap.compute()}. The
 * map holds that bin's lock for the duration, so two clients incrementing the same
 * counter can never lose an update — the NFR the PRD calls out explicitly.
 */
public final class StringCommands {

    private StringCommands() {
    }

    public static void register(CommandTable table) {
        table.register("get", 2, StringCommands::get);
        table.register("set", -3, StringCommands::set, Flag.WRITE);
        table.register("setnx", 3, StringCommands::setnx, Flag.WRITE);
        table.register("setex", 4, ctx -> setWithTtl(ctx, 1000L), Flag.WRITE);
        table.register("psetex", 4, ctx -> setWithTtl(ctx, 1L), Flag.WRITE);
        table.register("getset", 3, StringCommands::getset, Flag.WRITE);
        table.register("append", 3, StringCommands::append, Flag.WRITE);
        table.register("strlen", 2, StringCommands::strlen);
        table.register("incr", 2, ctx -> incrementBy(ctx, 1L), Flag.WRITE);
        table.register("decr", 2, ctx -> incrementBy(ctx, -1L), Flag.WRITE);
        table.register("incrby", 3, ctx -> incrementBy(ctx, ctx.longArg(2)), Flag.WRITE);
        table.register("decrby", 3, ctx -> incrementBy(ctx, negate(ctx.longArg(2))), Flag.WRITE);
        table.register("incrbyfloat", 3, StringCommands::incrByFloat, Flag.WRITE);
        table.register("mset", -3, StringCommands::mset, Flag.WRITE);
        table.register("msetnx", -3, StringCommands::msetnx, Flag.WRITE);
        table.register("mget", -2, StringCommands::mget);
    }

    private static long negate(long value) {
        if (value == Long.MIN_VALUE) {
            throw new RedisException("ERR decrement would overflow");
        }
        return -value;
    }

    static RespValue get(CommandContext ctx) {
        String value = ctx.db().getString(ctx.key());
        if (value == null) {
            ctx.server().recordMiss();
            return RespValue.NIL;
        }
        ctx.server().recordHit();
        return RespValue.bulk(value);
    }

    /**
     * {@code SET key value [EX s | PX ms | EXAT ts | PXAT ts | KEEPTTL] [NX | XX] [GET]}
     */
    static RespValue set(CommandContext ctx) {
        String key = ctx.key();
        String value = ctx.arg(2);
        boolean nx = false;
        boolean xx = false;
        boolean keepTtl = false;
        boolean returnOld = false;
        Long expireAt = null;

        for (int i = 3; i < ctx.argc(); i++) {
            String option = ctx.option(i);
            switch (option) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "KEEPTTL" -> keepTtl = true;
                case "GET" -> returnOld = true;
                case "EX", "PX", "EXAT", "PXAT" -> {
                    if (i + 1 >= ctx.argc()) {
                        throw RedisException.syntaxError();
                    }
                    long amount = ctx.longArg(++i);
                    expireAt = switch (option) {
                        case "EX" -> requirePositive(amount) * 1000L + System.currentTimeMillis();
                        case "PX" -> requirePositive(amount) + System.currentTimeMillis();
                        case "EXAT" -> amount * 1000L;
                        default -> amount;
                    };
                }
                default -> throw RedisException.syntaxError();
            }
        }
        if (nx && xx) {
            throw RedisException.syntaxError();
        }

        final boolean onlyIfAbsent = nx;
        final boolean onlyIfPresent = xx;
        final String newValue = value;
        AtomicReference<String> previous = new AtomicReference<>();
        AtomicBoolean applied = new AtomicBoolean();

        ctx.db().updateString(key, current -> {
            previous.set(current);
            if ((onlyIfAbsent && current != null) || (onlyIfPresent && current == null)) {
                return current;
            }
            applied.set(true);
            return newValue;
        });

        if (applied.get()) {
            if (expireAt != null) {
                ctx.db().setExpireAt(key, expireAt);
            } else if (!keepTtl) {
                ctx.db().persist(key);
            }
        }
        if (returnOld) {
            return RespValue.bulk(previous.get());
        }
        return applied.get() ? RespValue.OK : RespValue.NIL;
    }

    private static long requirePositive(long value) {
        if (value <= 0) {
            throw new RedisException("ERR invalid expire time in 'set' command");
        }
        return value;
    }

    static RespValue setnx(CommandContext ctx) {
        AtomicBoolean applied = new AtomicBoolean();
        String value = ctx.arg(2);
        ctx.db().updateString(ctx.key(), current -> {
            if (current != null) {
                return current;
            }
            applied.set(true);
            return value;
        });
        return RespValue.bool(applied.get());
    }

    /** Backs SETEX (seconds) and PSETEX (millis) via the multiplier. */
    static RespValue setWithTtl(CommandContext ctx, long unitMillis) {
        long ttl = ctx.longArg(2);
        if (ttl <= 0) {
            throw new RedisException("ERR invalid expire time in '" + ctx.name().toLowerCase() + "' command");
        }
        ctx.db().setString(ctx.key(), ctx.arg(3));
        ctx.db().setExpireAt(ctx.key(), System.currentTimeMillis() + ttl * unitMillis);
        return RespValue.OK;
    }

    static RespValue getset(CommandContext ctx) {
        AtomicReference<String> previous = new AtomicReference<>();
        String value = ctx.arg(2);
        ctx.db().updateString(ctx.key(), current -> {
            previous.set(current);
            return value;
        });
        ctx.db().persist(ctx.key());
        return RespValue.bulk(previous.get());
    }

    static RespValue append(CommandContext ctx) {
        String suffix = ctx.arg(2);
        String updated = ctx.db().updateString(ctx.key(),
                current -> current == null ? suffix : current + suffix);
        return RespValue.integer(updated.length());
    }

    static RespValue strlen(CommandContext ctx) {
        String value = ctx.db().getString(ctx.key());
        return RespValue.integer(value == null ? 0 : value.length());
    }

    static RespValue incrementBy(CommandContext ctx, long delta) {
        String updated = ctx.db().updateString(ctx.key(), current -> {
            long base = current == null ? 0L : strictLong(current);
            long next = base + delta;
            // Signed overflow check: the sum flipped sign against both operands.
            if (((base ^ next) & (delta ^ next)) < 0) {
                throw new RedisException("ERR increment or decrement would overflow");
            }
            return Long.toString(next);
        });
        return RespValue.integer(Long.parseLong(updated));
    }

    static RespValue incrByFloat(CommandContext ctx) {
        double delta = ctx.doubleArg(2);
        String updated = ctx.db().updateString(ctx.key(), current -> {
            double base = current == null ? 0.0 : strictDouble(current);
            double next = base + delta;
            if (Double.isNaN(next) || Double.isInfinite(next)) {
                throw new RedisException("ERR increment would produce NaN or Infinity");
            }
            return RespValue.formatDouble(next);
        });
        return RespValue.bulk(updated);
    }

    private static long strictLong(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw RedisException.notAnInteger();
        }
    }

    private static double strictDouble(String text) {
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw RedisException.notAFloat();
        }
    }

    static RespValue mset(CommandContext ctx) {
        if ((ctx.argc() - 1) % 2 != 0) {
            throw RedisException.wrongArgs("mset");
        }
        for (int i = 1; i < ctx.argc(); i += 2) {
            ctx.db().setString(ctx.arg(i), ctx.arg(i + 1));
        }
        return RespValue.OK;
    }

    static RespValue msetnx(CommandContext ctx) {
        if ((ctx.argc() - 1) % 2 != 0) {
            throw RedisException.wrongArgs("msetnx");
        }
        Database db = ctx.db();
        for (int i = 1; i < ctx.argc(); i += 2) {
            if (db.exists(ctx.arg(i))) {
                return RespValue.ZERO;
            }
        }
        for (int i = 1; i < ctx.argc(); i += 2) {
            db.setString(ctx.arg(i), ctx.arg(i + 1));
        }
        return RespValue.ONE;
    }

    static RespValue mget(CommandContext ctx) {
        List<RespValue> replies = new ArrayList<>(ctx.argc() - 1);
        for (int i = 1; i < ctx.argc(); i++) {
            try {
                replies.add(RespValue.bulk(ctx.db().getString(ctx.arg(i))));
            } catch (WrongTypeException e) {
                // MGET reports a non-string key as nil rather than failing the batch.
                replies.add(RespValue.NIL);
            }
        }
        return RespValue.array(replies);
    }
}
