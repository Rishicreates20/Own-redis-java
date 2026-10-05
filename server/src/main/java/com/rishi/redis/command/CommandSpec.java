package com.rishi.redis.command;

import java.util.Set;

/**
 * A command table entry: its name, its arity contract, the behavioural flags the
 * dispatcher needs, and the handler itself.
 *
 * @param arity number of arguments including the command name; a negative value
 *              means "at least {@code -arity}", exactly as Redis specifies it
 */
public record CommandSpec(String name, int arity, Set<Flag> flags, CommandHandler handler) {

    public enum Flag {
        /** Mutates the keyspace (used for stats and future replication hooks). */
        WRITE,
        /** Executes immediately even inside MULTI instead of being queued. */
        NO_QUEUE,
        /** Permitted while the connection is in subscriber mode. */
        SUBSCRIBER_SAFE,
        /** Permitted before AUTH succeeds. */
        NO_AUTH,
        /** Does not touch the keyspace, so the dispatcher skips the database lock. */
        NO_LOCK
    }

    public boolean arityMatches(int argc) {
        return arity >= 0 ? argc == arity : argc >= -arity;
    }

    public boolean has(Flag flag) {
        return flags.contains(flag);
    }

    public boolean isWrite() {
        return has(Flag.WRITE);
    }
}
