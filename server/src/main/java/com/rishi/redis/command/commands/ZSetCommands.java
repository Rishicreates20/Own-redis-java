package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.store.RedisZSet;
import com.rishi.redis.store.RedisZSet.ScoreBound;
import com.rishi.redis.store.RedisZSet.ScoredMember;

import java.util.ArrayList;
import java.util.List;

/**
 * FR-5 — sorted sets. Ranges come straight off the skip list's navigation methods,
 * which is what makes ordered traversal O(log n) to locate plus O(k) to walk.
 */
public final class ZSetCommands {

    private ZSetCommands() {
    }

    public static void register(CommandTable table) {
        table.register("zadd", -4, ZSetCommands::zadd, Flag.WRITE);
        table.register("zincrby", 4, ZSetCommands::zincrby, Flag.WRITE);
        table.register("zrem", -3, ZSetCommands::zrem, Flag.WRITE);
        table.register("zscore", 3, ZSetCommands::zscore);
        table.register("zcard", 2, ZSetCommands::zcard);
        table.register("zcount", 4, ZSetCommands::zcount);
        table.register("zrank", 3, ctx -> zrank(ctx, false));
        table.register("zrevrank", 3, ctx -> zrank(ctx, true));
        table.register("zrange", -4, ctx -> zrange(ctx, false));
        table.register("zrevrange", -4, ctx -> zrange(ctx, true));
        table.register("zrangebyscore", -4, ctx -> zrangeByScore(ctx, false));
        table.register("zrevrangebyscore", -4, ctx -> zrangeByScore(ctx, true));
        table.register("zpopmin", -2, ctx -> zpop(ctx, true), Flag.WRITE);
        table.register("zpopmax", -2, ctx -> zpop(ctx, false), Flag.WRITE);
    }

    /** {@code ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]} */
    static RespValue zadd(CommandContext ctx) {
        int cursor = 2;
        boolean nx = false;
        boolean xx = false;
        boolean gt = false;
        boolean lt = false;
        boolean changed = false;
        boolean incr = false;

        while (cursor < ctx.argc()) {
            String option = ctx.option(cursor);
            boolean consumed = true;
            switch (option) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GT" -> gt = true;
                case "LT" -> lt = true;
                case "CH" -> changed = true;
                case "INCR" -> incr = true;
                default -> consumed = false;
            }
            if (!consumed) {
                break;
            }
            cursor++;
        }
        if (nx && xx) {
            throw RedisException.syntaxError();
        }
        if (nx && (gt || lt)) {
            throw new RedisException("ERR GT, LT, and/or NX options at the same time are not compatible");
        }
        int remaining = ctx.argc() - cursor;
        if (remaining <= 0 || remaining % 2 != 0) {
            throw RedisException.syntaxError();
        }
        if (incr && remaining != 2) {
            throw new RedisException("ERR INCR option supports a single increment-element pair");
        }

        RedisZSet zset = ctx.db().zsetForWrite(ctx.key());
        long added = 0;
        long updated = 0;
        Double incrementResult = null;

        for (int i = cursor; i < ctx.argc(); i += 2) {
            double score = ctx.doubleArg(i);
            String member = ctx.arg(i + 1);
            Double existing = zset.score(member);

            if (nx && existing != null) {
                continue;
            }
            if (xx && existing == null) {
                if (incr) {
                    incrementResult = null;
                }
                continue;
            }
            if (incr) {
                if (existing != null && ((gt && score < 0) || (lt && score > 0))) {
                    continue;
                }
                incrementResult = zset.incrementBy(member, score);
                updated++;
                continue;
            }
            if (existing != null && ((gt && score <= existing) || (lt && score >= existing))) {
                continue;
            }
            if (zset.add(member, score)) {
                added++;
            } else if (existing == null || existing != score) {
                updated++;
            }
        }
        ctx.db().removeIfEmpty(ctx.key(), zset);

        if (incr) {
            return incrementResult == null ? RespValue.NIL : RespValue.bulkDouble(incrementResult);
        }
        return RespValue.integer(changed ? added + updated : added);
    }

    static RespValue zincrby(CommandContext ctx) {
        double delta = ctx.doubleArg(2);
        RedisZSet zset = ctx.db().zsetForWrite(ctx.key());
        return RespValue.bulkDouble(zset.incrementBy(ctx.arg(3), delta));
    }

    static RespValue zrem(CommandContext ctx) {
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        if (zset == null) {
            return RespValue.ZERO;
        }
        long removed = 0;
        for (int i = 2; i < ctx.argc(); i++) {
            if (zset.remove(ctx.arg(i))) {
                removed++;
            }
        }
        if (removed > 0) {
            ctx.db().touch(ctx.key());
        }
        ctx.db().removeIfEmpty(ctx.key(), zset);
        return RespValue.integer(removed);
    }

    static RespValue zscore(CommandContext ctx) {
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        Double score = zset == null ? null : zset.score(ctx.arg(2));
        return score == null ? RespValue.NIL : RespValue.bulkDouble(score);
    }

    static RespValue zcard(CommandContext ctx) {
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        return RespValue.integer(zset == null ? 0 : zset.size());
    }

    static RespValue zcount(CommandContext ctx) {
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        if (zset == null) {
            return RespValue.ZERO;
        }
        return RespValue.integer(zset.count(ScoreBound.parse(ctx.arg(2)), ScoreBound.parse(ctx.arg(3))));
    }

    static RespValue zrank(CommandContext ctx, boolean reverse) {
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        Long rank = zset == null ? null : zset.rank(ctx.arg(2), reverse);
        return rank == null ? RespValue.NIL : RespValue.integer(rank);
    }

    /** {@code ZRANGE key start stop [WITHSCORES]} (and the REV variant). */
    static RespValue zrange(CommandContext ctx, boolean reverse) {
        boolean withScores = hasWithScores(ctx, 4);
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        if (zset == null) {
            return RespValue.emptyArray();
        }
        return encode(zset.range(ctx.longArg(2), ctx.longArg(3), reverse), withScores);
    }

    /**
     * {@code ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]}. The REV
     * variant takes its bounds in the opposite order, as Redis does.
     */
    static RespValue zrangeByScore(CommandContext ctx, boolean reverse) {
        boolean withScores = false;
        long offset = 0;
        long count = -1;
        for (int i = 4; i < ctx.argc(); i++) {
            switch (ctx.option(i)) {
                case "WITHSCORES" -> withScores = true;
                case "LIMIT" -> {
                    if (i + 2 >= ctx.argc()) {
                        throw RedisException.syntaxError();
                    }
                    offset = ctx.longArg(++i);
                    count = ctx.longArg(++i);
                    if (offset < 0) {
                        throw RedisException.syntaxError();
                    }
                }
                default -> throw RedisException.syntaxError();
            }
        }
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        if (zset == null) {
            return RespValue.emptyArray();
        }
        ScoreBound min = ScoreBound.parse(ctx.arg(reverse ? 3 : 2));
        ScoreBound max = ScoreBound.parse(ctx.arg(reverse ? 2 : 3));
        return encode(zset.rangeByScore(min, max, reverse, offset, count), withScores);
    }

    static RespValue zpop(CommandContext ctx, boolean lowest) {
        long count = ctx.argc() == 3 ? ctx.longArg(2) : 1L;
        if (ctx.argc() > 3) {
            throw RedisException.wrongArgs(ctx.name().toLowerCase());
        }
        RedisZSet zset = ctx.db().zsetForRead(ctx.key());
        if (zset == null) {
            return RespValue.emptyArray();
        }
        List<RespValue> out = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            ScoredMember popped = lowest ? zset.pollFirst() : zset.pollLast();
            if (popped == null) {
                break;
            }
            out.add(RespValue.bulk(popped.member()));
            out.add(RespValue.bulkDouble(popped.score()));
        }
        if (!out.isEmpty()) {
            ctx.db().touch(ctx.key());
        }
        ctx.db().removeIfEmpty(ctx.key(), zset);
        return RespValue.array(out);
    }

    private static boolean hasWithScores(CommandContext ctx, int firstOptionIndex) {
        for (int i = firstOptionIndex; i < ctx.argc(); i++) {
            if (ctx.option(i).equals("WITHSCORES")) {
                continue;
            }
            throw RedisException.syntaxError();
        }
        return ctx.argc() > firstOptionIndex;
    }

    private static RespValue encode(List<ScoredMember> members, boolean withScores) {
        List<RespValue> out = new ArrayList<>(withScores ? members.size() * 2 : members.size());
        for (ScoredMember member : members) {
            out.add(RespValue.bulk(member.member()));
            if (withScores) {
                out.add(RespValue.bulkDouble(member.score()));
            }
        }
        return RespValue.array(out);
    }
}
