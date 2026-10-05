package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

/**
 * FR-4 — lists, backed by {@link java.util.concurrent.ConcurrentLinkedDeque}.
 *
 * <p>Pushes and pops at either end are lock-free CAS operations, which is exactly the
 * access pattern a Redis list gets hammered with (queues, job lists, capped logs).
 *
 * <p>The trade-off the structure imposes: LLEN and LRANGE walk the chain, so they are
 * O(n) rather than Redis's O(1)/O(offset). That is the price of lock-free ends; a
 * size counter beside the deque would fix LLEN but could drift from the true size,
 * and an indexed structure would put a lock back on the hot path.
 */
public final class ListCommands {

    private ListCommands() {
    }

    public static void register(CommandTable table) {
        table.register("lpush", -3, ctx -> push(ctx, true, false), Flag.WRITE);
        table.register("rpush", -3, ctx -> push(ctx, false, false), Flag.WRITE);
        table.register("lpushx", -3, ctx -> push(ctx, true, true), Flag.WRITE);
        table.register("rpushx", -3, ctx -> push(ctx, false, true), Flag.WRITE);
        table.register("lpop", -2, ctx -> pop(ctx, true), Flag.WRITE);
        table.register("rpop", -2, ctx -> pop(ctx, false), Flag.WRITE);
        table.register("llen", 2, ListCommands::llen);
        table.register("lrange", 4, ListCommands::lrange);
        table.register("lindex", 3, ListCommands::lindex);
        table.register("lrem", 4, ListCommands::lrem, Flag.WRITE);
    }

    /** @param requireExisting true for the LPUSHX/RPUSHX variants */
    static RespValue push(CommandContext ctx, boolean left, boolean requireExisting) {
        if (requireExisting && ctx.db().listForRead(ctx.key()) == null) {
            return RespValue.ZERO;
        }
        Deque<String> list = ctx.db().listForWrite(ctx.key());
        for (int i = 2; i < ctx.argc(); i++) {
            if (left) {
                list.addFirst(ctx.arg(i));
            } else {
                list.addLast(ctx.arg(i));
            }
        }
        return RespValue.integer(list.size());
    }

    /** {@code LPOP key [count]} / {@code RPOP key [count]}. */
    static RespValue pop(CommandContext ctx, boolean left) {
        if (ctx.argc() > 3) {
            throw RedisException.wrongArgs(ctx.name().toLowerCase());
        }
        Deque<String> list = ctx.db().listForRead(ctx.key());
        boolean counted = ctx.argc() == 3;
        long count = counted ? ctx.longArg(2) : 1L;
        if (count < 0) {
            throw RedisException.outOfRange();
        }
        if (list == null) {
            return counted ? RespValue.NIL_ARRAY : RespValue.NIL;
        }

        List<RespValue> popped = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            String element = left ? list.pollFirst() : list.pollLast();
            if (element == null) {
                break;
            }
            popped.add(RespValue.bulk(element));
        }
        if (!popped.isEmpty()) {
            ctx.db().touch(ctx.key());
        }
        ctx.db().removeIfEmpty(ctx.key(), list);

        if (counted) {
            return RespValue.array(popped);
        }
        return popped.isEmpty() ? RespValue.NIL : popped.get(0);
    }

    static RespValue llen(CommandContext ctx) {
        Deque<String> list = ctx.db().listForRead(ctx.key());
        return RespValue.integer(list == null ? 0 : list.size());
    }

    static RespValue lrange(CommandContext ctx) {
        Deque<String> list = ctx.db().listForRead(ctx.key());
        if (list == null) {
            return RespValue.emptyArray();
        }
        List<String> snapshot = new ArrayList<>(list);
        int size = snapshot.size();
        long start = normalise(ctx.longArg(2), size);
        long stop = normalise(ctx.longArg(3), size);
        if (start < 0) {
            start = 0;
        }
        if (stop >= size) {
            stop = size - 1L;
        }
        if (start > stop || start >= size) {
            return RespValue.emptyArray();
        }
        return RespValue.bulkArray(snapshot.subList((int) start, (int) stop + 1));
    }

    static RespValue lindex(CommandContext ctx) {
        Deque<String> list = ctx.db().listForRead(ctx.key());
        if (list == null) {
            return RespValue.NIL;
        }
        List<String> snapshot = new ArrayList<>(list);
        long index = normalise(ctx.longArg(2), snapshot.size());
        if (index < 0 || index >= snapshot.size()) {
            return RespValue.NIL;
        }
        return RespValue.bulk(snapshot.get((int) index));
    }

    /**
     * {@code LREM key count value}: positive count removes from the head, negative from
     * the tail, zero removes every match.
     */
    static RespValue lrem(CommandContext ctx) {
        Deque<String> list = ctx.db().listForRead(ctx.key());
        if (list == null) {
            return RespValue.ZERO;
        }
        long count = ctx.longArg(2);
        String target = ctx.arg(3);
        long limit = count == 0 ? Long.MAX_VALUE : Math.abs(count);

        long removed = 0;
        Iterator<String> iterator = count < 0 ? list.descendingIterator() : list.iterator();
        while (iterator.hasNext() && removed < limit) {
            if (target.equals(iterator.next())) {
                iterator.remove();
                removed++;
            }
        }
        if (removed > 0) {
            ctx.db().touch(ctx.key());
        }
        ctx.db().removeIfEmpty(ctx.key(), list);
        return RespValue.integer(removed);
    }

    /** Turns a possibly negative Redis index into an absolute one. */
    private static long normalise(long index, int size) {
        return index < 0 ? size + index : index;
    }
}
