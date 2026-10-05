package com.rishi.redis.command;

import com.rishi.redis.RedisException;
import com.rishi.redis.server.ClientSession;
import com.rishi.redis.server.ServerContext;
import com.rishi.redis.store.Database;

import java.util.List;
import java.util.Locale;

/**
 * What a handler is given: the server, the calling connection, and the argument
 * vector (element 0 is the command name). The accessors here exist so that every
 * handler reports the same error text for the same mistake.
 */
public record CommandContext(ServerContext server, ClientSession session, List<String> args) {

    public String name() {
        return args.get(0);
    }

    public int argc() {
        return args.size();
    }

    public String arg(int index) {
        return args.get(index);
    }

    /** Uppercased argument, for option keywords such as {@code EX} or {@code WITHSCORES}. */
    public String option(int index) {
        return args.get(index).toUpperCase(Locale.ROOT);
    }

    /** The conventional position of the key. */
    public String key() {
        return args.get(1);
    }

    public Database db() {
        return session.database();
    }

    public long longArg(int index) {
        return parseLong(args.get(index));
    }

    public double doubleArg(int index) {
        return parseDouble(args.get(index));
    }

    public static long parseLong(String text) {
        try {
            return Long.parseLong(text.strip());
        } catch (NumberFormatException e) {
            throw RedisException.notAnInteger();
        }
    }

    public static double parseDouble(String text) {
        String body = text.strip();
        return switch (body.toLowerCase(Locale.ROOT)) {
            case "inf", "+inf", "infinity", "+infinity" -> Double.POSITIVE_INFINITY;
            case "-inf", "-infinity" -> Double.NEGATIVE_INFINITY;
            default -> {
                try {
                    double parsed = Double.parseDouble(body);
                    if (Double.isNaN(parsed)) {
                        throw RedisException.notAFloat();
                    }
                    yield parsed;
                } catch (NumberFormatException e) {
                    throw RedisException.notAFloat();
                }
            }
        };
    }
}
