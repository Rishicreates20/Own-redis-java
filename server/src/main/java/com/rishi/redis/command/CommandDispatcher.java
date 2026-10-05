package com.rishi.redis.command;

import com.rishi.redis.RedisException;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.server.ClientSession;
import com.rishi.redis.server.ServerContext;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.Lock;

/**
 * The gate every command passes through: lookup, arity, auth, MULTI queueing,
 * subscriber-mode restrictions, locking, and error translation.
 *
 * <p>Keeping all of that here means a handler only ever has to implement the actual
 * semantics of its command — it can assume it was called with a plausible number of
 * arguments by an authenticated client holding the right lock.
 */
public final class CommandDispatcher {

    private CommandDispatcher() {
    }

    /**
     * @return the reply, or {@code null} when the command answers the client itself
     *         (SUBSCRIBE) or deliberately says nothing (QUIT-in-flight)
     */
    public static RespValue dispatch(ServerContext server, ClientSession session, List<String> args) {
        if (args.isEmpty()) {
            return null;
        }
        session.touch();
        CommandSpec spec = server.commands().lookup(args.get(0));

        if (spec == null) {
            if (session.inMulti()) {
                session.abortTransaction();
            }
            return RespValue.error("ERR unknown command '" + args.get(0)
                    + "', with args beginning with: " + preview(args));
        }
        if (!spec.arityMatches(args.size())) {
            if (session.inMulti()) {
                session.abortTransaction();
            }
            return RespValue.error("ERR wrong number of arguments for '" + spec.name() + "' command");
        }
        if (server.config().requiresAuth() && !session.isAuthenticated()
                && !spec.has(CommandSpec.Flag.NO_AUTH)) {
            return RespValue.error("NOAUTH Authentication required.");
        }

        // FR-7: inside MULTI every ordinary command is only recorded, never run.
        if (session.inMulti() && !spec.has(CommandSpec.Flag.NO_QUEUE)) {
            session.queue(args);
            return RespValue.QUEUED;
        }

        if (session.inSubscriberMode() && !spec.has(CommandSpec.Flag.SUBSCRIBER_SAFE)) {
            return RespValue.error("ERR Can't execute '" + spec.name()
                    + "': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context");
        }

        return execute(server, session, spec, args);
    }

    /**
     * Runs a command that has already cleared the gate. EXEC re-enters here for each
     * queued command while holding the write lock — a {@code ReentrantReadWriteLock}
     * lets the same thread take the read lock it already dominates, so the nested
     * acquisition below is a no-op rather than a deadlock.
     */
    public static RespValue execute(ServerContext server, ClientSession session,
                                    CommandSpec spec, List<String> args) {
        server.recordCommand();
        CommandContext ctx = new CommandContext(server, session, args);
        Lock lock = spec.has(CommandSpec.Flag.NO_LOCK) ? null : session.database().commandLock();
        if (lock != null) {
            lock.lock();
        }
        try {
            RespValue reply = spec.handler().execute(ctx);
            if (spec.isWrite()) {
                server.snapshots().recordChange();
            }
            return reply;
        } catch (RedisException e) {
            return RespValue.error(e.getMessage());
        } catch (IllegalArgumentException e) {
            return RespValue.error("ERR " + e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            return RespValue.error("ERR wrong number of arguments for '" + spec.name() + "' command");
        } catch (RuntimeException e) {
            // A bug in one handler must not take the connection — or the server — down.
            System.err.println("[command] " + spec.name() + " failed: " + e);
            return RespValue.error("ERR internal error executing '" + spec.name() + "'");
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    private static String preview(List<String> args) {
        StringBuilder text = new StringBuilder();
        for (int i = 1; i < Math.min(args.size(), 4); i++) {
            if (i > 1) {
                text.append(", ");
            }
            String arg = args.get(i);
            text.append('\'').append(arg.length() > 32 ? arg.substring(0, 32) + "..." : arg).append('\'');
        }
        return text.toString();
    }

    /** Normalised command name, used by logging and the HTTP bridge. */
    public static String normalise(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
