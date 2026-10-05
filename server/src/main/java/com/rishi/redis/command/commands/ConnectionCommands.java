package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;

import java.util.ArrayList;
import java.util.List;

/**
 * FR-3 — connection-level commands: the handshake surface every Redis client pokes
 * at before it will talk to a server.
 */
public final class ConnectionCommands {

    public static final String SERVER_NAME = "own-redis-java";
    public static final String SERVER_VERSION = "1.0.0";

    private ConnectionCommands() {
    }

    public static void register(CommandTable table) {
        table.register("ping", -1, ConnectionCommands::ping,
                Flag.NO_LOCK, Flag.SUBSCRIBER_SAFE, Flag.NO_AUTH);
        table.register("echo", 2, ConnectionCommands::echo, Flag.NO_LOCK);
        table.register("select", 2, ConnectionCommands::select, Flag.NO_LOCK);
        table.register("swapdb", 3, ctx -> {
            throw new RedisException("ERR SWAPDB is not supported");
        }, Flag.NO_LOCK);
        table.register("auth", -2, ConnectionCommands::auth,
                Flag.NO_LOCK, Flag.NO_AUTH, Flag.SUBSCRIBER_SAFE);
        table.register("hello", -1, ConnectionCommands::hello,
                Flag.NO_LOCK, Flag.NO_AUTH, Flag.SUBSCRIBER_SAFE);
        table.register("quit", -1, ConnectionCommands::quit,
                Flag.NO_LOCK, Flag.NO_AUTH, Flag.SUBSCRIBER_SAFE, Flag.NO_QUEUE);
        table.register("reset", 1, ConnectionCommands::reset,
                Flag.NO_LOCK, Flag.NO_AUTH, Flag.SUBSCRIBER_SAFE, Flag.NO_QUEUE);
        table.register("client", -2, ConnectionCommands::client, Flag.NO_LOCK, Flag.SUBSCRIBER_SAFE);
    }

    static RespValue ping(CommandContext ctx) {
        if (ctx.argc() > 2) {
            throw RedisException.wrongArgs("ping");
        }
        return ctx.argc() == 2 ? RespValue.bulk(ctx.arg(1)) : RespValue.PONG;
    }

    static RespValue echo(CommandContext ctx) {
        return RespValue.bulk(ctx.arg(1));
    }

    static RespValue select(CommandContext ctx) {
        long index = ctx.longArg(1);
        if (index < 0 || index >= ctx.server().databaseCount()) {
            throw new RedisException("ERR DB index is out of range");
        }
        ctx.session().select((int) index);
        return RespValue.OK;
    }

    static RespValue auth(CommandContext ctx) {
        if (ctx.argc() > 3) {
            throw RedisException.wrongArgs("auth");
        }
        // Redis 6+ accepts AUTH <user> <pass>; only the default user exists here.
        String supplied = ctx.arg(ctx.argc() - 1);
        if (!ctx.server().config().requiresAuth()) {
            throw new RedisException(
                    "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?");
        }
        if (!constantTimeEquals(supplied, ctx.server().config().password())) {
            ctx.session().setAuthenticated(false);
            throw new RedisException("WRONGPASS invalid username-password pair or user is disabled.");
        }
        ctx.session().setAuthenticated(true);
        return RespValue.OK;
    }

    /** Avoids leaking password length/prefix through response timing. */
    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        int difference = a.length() ^ b.length();
        for (int i = 0; i < a.length() && i < b.length(); i++) {
            difference |= a.charAt(i) ^ b.charAt(i);
        }
        return difference == 0;
    }

    /**
     * RESP3's handshake. This server speaks RESP2, so it answers HELLO (or HELLO 2)
     * with the standard field list and refuses a request for protocol 3 — which is
     * exactly what a client needs in order to fall back cleanly.
     */
    static RespValue hello(CommandContext ctx) {
        if (ctx.argc() >= 2) {
            long version = ctx.longArg(1);
            if (version != 2) {
                throw new RedisException("NOPROTO unsupported protocol version");
            }
        }
        List<RespValue> reply = new ArrayList<>();
        reply.add(RespValue.bulk("server"));
        reply.add(RespValue.bulk(SERVER_NAME));
        reply.add(RespValue.bulk("version"));
        reply.add(RespValue.bulk(SERVER_VERSION));
        reply.add(RespValue.bulk("proto"));
        reply.add(RespValue.integer(2));
        reply.add(RespValue.bulk("id"));
        reply.add(RespValue.bulk(ctx.session().id()));
        reply.add(RespValue.bulk("mode"));
        reply.add(RespValue.bulk("standalone"));
        reply.add(RespValue.bulk("role"));
        reply.add(RespValue.bulk("master"));
        reply.add(RespValue.bulk("modules"));
        reply.add(RespValue.emptyArray());
        return RespValue.array(reply);
    }

    static RespValue quit(CommandContext ctx) {
        ctx.session().requestClose();
        return RespValue.OK;
    }

    /** RESET returns the connection to a clean slate: no MULTI, no watches, db 0. */
    static RespValue reset(CommandContext ctx) {
        ctx.session().endMulti();
        ctx.server().pubsub().removeAll(ctx.session());
        ctx.session().select(0);
        ctx.session().setName("");
        if (ctx.server().config().requiresAuth()) {
            ctx.session().setAuthenticated(false);
        }
        return RespValue.simple("RESET");
    }

    static RespValue client(CommandContext ctx) {
        String subcommand = ctx.option(1);
        return switch (subcommand) {
            case "ID" -> RespValue.bulk(ctx.session().id());
            case "GETNAME" -> {
                String name = ctx.session().name();
                yield name.isEmpty() ? RespValue.NIL : RespValue.bulk(name);
            }
            case "SETNAME" -> {
                if (ctx.argc() != 3) {
                    throw RedisException.wrongArgs("client|setname");
                }
                ctx.session().setName(ctx.arg(2));
                yield RespValue.OK;
            }
            case "LIST" -> {
                StringBuilder text = new StringBuilder();
                ctx.server().sessions().forEach(session -> text
                        .append("id=").append(session.id())
                        .append(" addr=").append(session.output().remoteAddress())
                        .append(" name=").append(session.name())
                        .append(" db=").append(session.databaseIndex())
                        .append(" sub=").append(session.subscriptionCount())
                        .append(" age=").append((System.currentTimeMillis() - session.createdAt()) / 1000)
                        .append('\n'));
                yield RespValue.bulk(text.toString());
            }
            case "INFO" -> RespValue.bulk("id=" + ctx.session().id()
                    + " addr=" + ctx.session().output().remoteAddress()
                    + " db=" + ctx.session().databaseIndex());
            default -> throw new RedisException(
                    "ERR Unknown CLIENT subcommand or wrong number of arguments for '" + ctx.arg(1) + "'");
        };
    }
}
