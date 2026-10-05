package com.rishi.redis.command.commands;

import com.rishi.redis.command.CommandDispatcher;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.server.ClientSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * FR-7 / TD-7 — MULTI, EXEC, DISCARD, WATCH.
 *
 * <p>MULTI flips the connection into queuing mode: the dispatcher records subsequent
 * commands and answers {@code +QUEUED} without touching the keyspace. EXEC then runs
 * the whole batch while holding the database's <em>write</em> lock, so no other client
 * can interleave a command into the middle of the transaction — ordinary commands only
 * ever hold the read lock.
 *
 * <p>WATCH is optimistic rather than pessimistic: it records the version counter of
 * each watched key, and EXEC compares them before running. If anyone touched a watched
 * key in between, the transaction is abandoned and the client gets a nil reply — no
 * lock is held across the client's think time, which is what makes check-and-set
 * workloads scale.
 */
public final class TransactionCommands {

    private TransactionCommands() {
    }

    public static void register(CommandTable table) {
        table.register("multi", 1, TransactionCommands::multi, Flag.NO_QUEUE, Flag.NO_LOCK);
        table.register("exec", 1, TransactionCommands::exec, Flag.NO_QUEUE, Flag.NO_LOCK);
        table.register("discard", 1, TransactionCommands::discard, Flag.NO_QUEUE, Flag.NO_LOCK);
        table.register("watch", -2, TransactionCommands::watch, Flag.NO_QUEUE, Flag.NO_LOCK);
        table.register("unwatch", 1, TransactionCommands::unwatch, Flag.NO_QUEUE, Flag.NO_LOCK);
    }

    static RespValue multi(CommandContext ctx) {
        if (ctx.session().inMulti()) {
            return RespValue.error("ERR MULTI calls can not be nested");
        }
        ctx.session().beginMulti();
        return RespValue.OK;
    }

    static RespValue discard(CommandContext ctx) {
        if (!ctx.session().inMulti()) {
            return RespValue.error("ERR DISCARD without MULTI");
        }
        ctx.session().endMulti();
        return RespValue.OK;
    }

    static RespValue watch(CommandContext ctx) {
        if (ctx.session().inMulti()) {
            return RespValue.error("ERR WATCH inside MULTI is not allowed");
        }
        for (int i = 1; i < ctx.argc(); i++) {
            ctx.session().watch(ctx.arg(i));
        }
        return RespValue.OK;
    }

    static RespValue unwatch(CommandContext ctx) {
        ctx.session().unwatchAll();
        return RespValue.OK;
    }

    static RespValue exec(CommandContext ctx) {
        ClientSession session = ctx.session();
        if (!session.inMulti()) {
            return RespValue.error("ERR EXEC without MULTI");
        }
        if (session.isTransactionAborted()) {
            session.endMulti();
            return RespValue.error("EXECABORT Transaction discarded because of previous errors.");
        }

        Lock lock = session.database().transactionLock();
        lock.lock();
        try {
            if (!session.watchesIntact()) {
                // A watched key moved: abandon the batch and report nil, as Redis does.
                session.endMulti();
                return RespValue.NIL_ARRAY;
            }
            List<List<String>> batch = List.copyOf(session.queuedCommands());
            session.endMulti();

            List<RespValue> replies = new ArrayList<>(batch.size());
            for (List<String> args : batch) {
                CommandSpec spec = ctx.server().commands().lookup(args.get(0));
                if (spec == null) {
                    replies.add(RespValue.error("ERR unknown command '" + args.get(0) + "'"));
                    continue;
                }
                RespValue reply = CommandDispatcher.execute(ctx.server(), session, spec, args);
                replies.add(reply == null ? RespValue.OK : reply);
            }
            return RespValue.array(replies);
        } finally {
            lock.unlock();
        }
    }
}
