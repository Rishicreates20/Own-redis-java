package com.rishi.redis.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * The RESP2 reply model. Five types, exactly as they appear on the wire.
 *
 * <p>Strings held here are <em>wire strings</em> (see {@link Bytes}).
 */
public sealed interface RespValue {

    /** {@code +OK\r\n} */
    record SimpleString(String value) implements RespValue {
    }

    /** {@code -ERR something\r\n} */
    record Error(String message) implements RespValue {
    }

    /** {@code :42\r\n} */
    record Integer64(long value) implements RespValue {
    }

    /** {@code $5\r\nhello\r\n}; a {@code null} value encodes the RESP nil bulk string. */
    record BulkString(String value) implements RespValue {
    }

    /** {@code *2\r\n...}; {@code null} items encode the RESP nil array. */
    record Array(List<RespValue> items) implements RespValue {
    }

    RespValue OK = new SimpleString("OK");
    RespValue PONG = new SimpleString("PONG");
    RespValue QUEUED = new SimpleString("QUEUED");
    RespValue NIL = new BulkString(null);
    RespValue NIL_ARRAY = new Array(null);
    RespValue ZERO = new Integer64(0L);
    RespValue ONE = new Integer64(1L);

    static RespValue simple(String value) {
        return new SimpleString(value);
    }

    static RespValue error(String message) {
        return new Error(message);
    }

    static RespValue integer(long value) {
        return value == 0L ? ZERO : value == 1L ? ONE : new Integer64(value);
    }

    static RespValue bool(boolean value) {
        return value ? ONE : ZERO;
    }

    static RespValue bulk(String value) {
        return value == null ? NIL : new BulkString(value);
    }

    /** A double formatted the way Redis formats scores: shortest round-trip form. */
    static RespValue bulkDouble(double value) {
        return bulk(formatDouble(value));
    }

    static RespValue array(List<RespValue> items) {
        return new Array(items);
    }

    static RespValue emptyArray() {
        return new Array(List.of());
    }

    /** Wraps each element as a bulk string; {@code null} elements become nil. */
    static RespValue bulkArray(List<String> values) {
        List<RespValue> items = new ArrayList<>(values.size());
        for (String value : values) {
            items.add(bulk(value));
        }
        return new Array(items);
    }

    static String formatDouble(double value) {
        if (value == Double.POSITIVE_INFINITY) {
            return "inf";
        }
        if (value == Double.NEGATIVE_INFINITY) {
            return "-inf";
        }
        if (value == Math.rint(value) && !Double.isNaN(value) && Math.abs(value) < 1e17) {
            return Long.toString((long) value);
        }
        String text = Double.toString(value);
        return text.endsWith(".0") ? text.substring(0, text.length() - 2) : text;
    }
}
