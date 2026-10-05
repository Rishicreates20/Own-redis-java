package com.rishi.redis.command.commands;

import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * FR-4 — sets, backed by {@code ConcurrentHashMap.newKeySet()}.
 *
 * <p>Membership tests and adds are lock-free; the algebraic commands (SUNION, SINTER,
 * SDIFF) copy their inputs first so a concurrent write cannot make the result
 * self-inconsistent halfway through.
 */
public final class SetCommands {

    private SetCommands() {
    }

    public static void register(CommandTable table) {
        table.register("sadd", -3, SetCommands::sadd, Flag.WRITE);
        table.register("srem", -3, SetCommands::srem, Flag.WRITE);
        table.register("smembers", 2, SetCommands::smembers);
        table.register("sismember", 3, SetCommands::sismember);
        table.register("scard", 2, SetCommands::scard);
        table.register("spop", -2, SetCommands::spop, Flag.WRITE);
        table.register("srandmember", -2, SetCommands::srandmember);
        table.register("sunion", -2, ctx -> combine(ctx, Combination.UNION));
        table.register("sinter", -2, ctx -> combine(ctx, Combination.INTERSECTION));
        table.register("sdiff", -2, ctx -> combine(ctx, Combination.DIFFERENCE));
        table.register("smove", 4, SetCommands::smove, Flag.WRITE);
    }

    private enum Combination {
        UNION, INTERSECTION, DIFFERENCE
    }

    static RespValue sadd(CommandContext ctx) {
        Set<String> set = ctx.db().setForWrite(ctx.key());
        long added = 0;
        for (int i = 2; i < ctx.argc(); i++) {
            if (set.add(ctx.arg(i))) {
                added++;
            }
        }
        return RespValue.integer(added);
    }

    static RespValue srem(CommandContext ctx) {
        Set<String> set = ctx.db().setForRead(ctx.key());
        if (set == null) {
            return RespValue.ZERO;
        }
        long removed = 0;
        for (int i = 2; i < ctx.argc(); i++) {
            if (set.remove(ctx.arg(i))) {
                removed++;
            }
        }
        if (removed > 0) {
            ctx.db().touch(ctx.key());
        }
        ctx.db().removeIfEmpty(ctx.key(), set);
        return RespValue.integer(removed);
    }

    static RespValue smembers(CommandContext ctx) {
        Set<String> set = ctx.db().setForRead(ctx.key());
        return set == null ? RespValue.emptyArray() : RespValue.bulkArray(new ArrayList<>(set));
    }

    static RespValue sismember(CommandContext ctx) {
        Set<String> set = ctx.db().setForRead(ctx.key());
        return RespValue.bool(set != null && set.contains(ctx.arg(2)));
    }

    static RespValue scard(CommandContext ctx) {
        Set<String> set = ctx.db().setForRead(ctx.key());
        return RespValue.integer(set == null ? 0 : set.size());
    }

    static RespValue spop(CommandContext ctx) {
        Set<String> set = ctx.db().setForRead(ctx.key());
        boolean counted = ctx.argc() == 3;
        long count = counted ? ctx.longArg(2) : 1L;
        if (count < 0) {
            throw com.rishi.redis.RedisException.outOfRange();
        }
        if (set == null) {
            return counted ? RespValue.emptyArray() : RespValue.NIL;
        }
        List<String> candidates = new ArrayList<>(set);
        List<RespValue> popped = new ArrayList<>();
        while (!candidates.isEmpty() && popped.size() < count) {
            String member = candidates.remove(ThreadLocalRandom.current().nextInt(candidates.size()));
            if (set.remove(member)) {
                popped.add(RespValue.bulk(member));
            }
        }
        if (!popped.isEmpty()) {
            ctx.db().touch(ctx.key());
        }
        ctx.db().removeIfEmpty(ctx.key(), set);
        if (counted) {
            return RespValue.array(popped);
        }
        return popped.isEmpty() ? RespValue.NIL : popped.get(0);
    }

    static RespValue srandmember(CommandContext ctx) {
        Set<String> set = ctx.db().setForRead(ctx.key());
        boolean counted = ctx.argc() == 3;
        long count = counted ? ctx.longArg(2) : 1L;
        if (set == null) {
            return counted ? RespValue.emptyArray() : RespValue.NIL;
        }
        List<String> members = new ArrayList<>(set);
        if (members.isEmpty()) {
            return counted ? RespValue.emptyArray() : RespValue.NIL;
        }
        if (!counted) {
            return RespValue.bulk(members.get(ThreadLocalRandom.current().nextInt(members.size())));
        }
        List<String> picked = new ArrayList<>();
        if (count < 0) {
            // A negative count may repeat members.
            for (long i = 0; i < -count; i++) {
                picked.add(members.get(ThreadLocalRandom.current().nextInt(members.size())));
            }
        } else {
            java.util.Collections.shuffle(members, ThreadLocalRandom.current());
            picked.addAll(members.subList(0, (int) Math.min(count, members.size())));
        }
        return RespValue.bulkArray(picked);
    }

    static RespValue combine(CommandContext ctx, Combination mode) {
        Set<String> accumulator = null;
        for (int i = 1; i < ctx.argc(); i++) {
            Set<String> operand = ctx.db().setForRead(ctx.arg(i));
            Set<String> copy = operand == null ? Set.of() : new LinkedHashSet<>(operand);
            if (accumulator == null) {
                accumulator = new LinkedHashSet<>(copy);
                continue;
            }
            switch (mode) {
                case UNION -> accumulator.addAll(copy);
                case INTERSECTION -> accumulator.retainAll(copy);
                case DIFFERENCE -> accumulator.removeAll(copy);
            }
        }
        return RespValue.bulkArray(new ArrayList<>(accumulator == null ? Set.of() : accumulator));
    }

    static RespValue smove(CommandContext ctx) {
        String source = ctx.arg(1);
        String destination = ctx.arg(2);
        String member = ctx.arg(3);
        Set<String> from = ctx.db().setForRead(source);
        if (from == null || !from.remove(member)) {
            return RespValue.ZERO;
        }
        ctx.db().touch(source);
        ctx.db().removeIfEmpty(source, from);
        ctx.db().setForWrite(destination).add(member);
        return RespValue.ONE;
    }
}
