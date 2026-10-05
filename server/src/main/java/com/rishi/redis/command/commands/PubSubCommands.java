package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.pubsub.PubSubRegistry;
import com.rishi.redis.server.ClientSession;

import java.util.ArrayList;
import java.util.List;

/**
 * FR-8 / TD-10 — Pub/Sub.
 *
 * <p>(P)SUBSCRIBE and (P)UNSUBSCRIBE answer the client themselves: each channel gets
 * its own confirmation frame, so these handlers push frames and return {@code null}
 * instead of a single reply.
 */
public final class PubSubCommands {

    private PubSubCommands() {
    }

    public static void register(CommandTable table) {
        table.register("subscribe", -2, ctx -> subscribe(ctx, false),
                Flag.NO_LOCK, Flag.SUBSCRIBER_SAFE);
        table.register("unsubscribe", -1, ctx -> unsubscribe(ctx, false),
                Flag.NO_LOCK, Flag.SUBSCRIBER_SAFE);
        table.register("psubscribe", -2, ctx -> subscribe(ctx, true),
                Flag.NO_LOCK, Flag.SUBSCRIBER_SAFE);
        table.register("punsubscribe", -1, ctx -> unsubscribe(ctx, true),
                Flag.NO_LOCK, Flag.SUBSCRIBER_SAFE);
        table.register("publish", 3, PubSubCommands::publish, Flag.NO_LOCK);
        table.register("pubsub", -2, PubSubCommands::pubsub, Flag.NO_LOCK, Flag.SUBSCRIBER_SAFE);
    }

    static RespValue subscribe(CommandContext ctx, boolean pattern) {
        PubSubRegistry registry = ctx.server().pubsub();
        ClientSession session = ctx.session();
        for (int i = 1; i < ctx.argc(); i++) {
            String target = ctx.arg(i);
            int count = pattern ? registry.psubscribe(session, target) : registry.subscribe(session, target);
            session.push(confirmation(pattern ? "psubscribe" : "subscribe", target, count));
        }
        return null;
    }

    static RespValue unsubscribe(CommandContext ctx, boolean pattern) {
        PubSubRegistry registry = ctx.server().pubsub();
        ClientSession session = ctx.session();

        List<String> targets = new ArrayList<>();
        if (ctx.argc() > 1) {
            for (int i = 1; i < ctx.argc(); i++) {
                targets.add(ctx.arg(i));
            }
        } else {
            targets.addAll(pattern ? session.patterns() : session.channels());
        }

        if (targets.isEmpty()) {
            // Redis still acknowledges, with a nil channel name and a zero count.
            session.push(RespValue.array(List.of(
                    RespValue.bulk(pattern ? "punsubscribe" : "unsubscribe"),
                    RespValue.NIL,
                    RespValue.integer(session.subscriptionCount()))));
            return null;
        }
        for (String target : targets) {
            int count = pattern
                    ? registry.punsubscribe(session, target)
                    : registry.unsubscribe(session, target);
            session.push(confirmation(pattern ? "punsubscribe" : "unsubscribe", target, count));
        }
        return null;
    }

    private static RespValue confirmation(String kind, String target, int count) {
        return RespValue.array(List.of(
                RespValue.bulk(kind),
                RespValue.bulk(target),
                RespValue.integer(count)));
    }

    static RespValue publish(CommandContext ctx) {
        return RespValue.integer(ctx.server().pubsub().publish(ctx.arg(1), ctx.arg(2)));
    }

    static RespValue pubsub(CommandContext ctx) {
        return switch (ctx.option(1)) {
            case "CHANNELS" -> {
                String pattern = ctx.argc() > 2 ? ctx.arg(2) : null;
                yield RespValue.bulkArray(ctx.server().pubsub().activeChannels(pattern));
            }
            case "NUMSUB" -> {
                List<RespValue> out = new ArrayList<>();
                for (int i = 2; i < ctx.argc(); i++) {
                    out.add(RespValue.bulk(ctx.arg(i)));
                    out.add(RespValue.integer(ctx.server().pubsub().subscriberCount(ctx.arg(i))));
                }
                yield RespValue.array(out);
            }
            case "NUMPAT" -> RespValue.integer(ctx.server().pubsub().patternCount());
            default -> throw new RedisException("ERR Unknown PUBSUB subcommand: " + ctx.arg(1));
        };
    }
}
