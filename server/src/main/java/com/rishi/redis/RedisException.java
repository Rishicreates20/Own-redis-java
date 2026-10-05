package com.rishi.redis;

/**
 * A command-level failure that is reported back to the client as a RESP error
 * reply ({@code -ERR ...}) rather than killing the connection.
 *
 * <p>The message is used verbatim on the wire, so it must already carry the
 * Redis error prefix, e.g. {@code "WRONGTYPE Operation against ..."} or
 * {@code "ERR value is not an integer or out of range"}.
 */
public class RedisException extends RuntimeException {

    public RedisException(String message) {
        super(message, null, false, false);
    }

    public static RedisException notAnInteger() {
        return new RedisException("ERR value is not an integer or out of range");
    }

    public static RedisException notAFloat() {
        return new RedisException("ERR value is not a valid float");
    }

    public static RedisException syntaxError() {
        return new RedisException("ERR syntax error");
    }

    public static RedisException wrongArgs(String command) {
        return new RedisException("ERR wrong number of arguments for '" + command + "' command");
    }

    public static RedisException outOfRange() {
        return new RedisException("ERR value is out of range, must be positive");
    }
}
