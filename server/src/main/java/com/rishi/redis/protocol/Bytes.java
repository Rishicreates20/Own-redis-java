package com.rishi.redis.protocol;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Binary-safety helper.
 *
 * <p>Redis is binary safe: any byte sequence may be a key or a value. Rather
 * than threading {@code byte[]} through every map (which then needs a wrapper
 * for {@code equals}/{@code hashCode}), the server stores <em>wire strings</em>:
 * a {@code String} produced by decoding the raw bytes as ISO-8859-1.
 *
 * <p>ISO-8859-1 is a total, lossless 1:1 mapping between the 256 byte values and
 * the first 256 code points, so {@code bytes -> String -> bytes} always round-trips
 * exactly. Every byte sequence therefore has a distinct wire string, and
 * {@code String}'s cached hash code and fast equality come for free.
 *
 * <p>Text arriving from a non-RESP source (the JSON/HTTP bridge, which is UTF-8)
 * must be converted with {@link #fromText(String)} on the way in and
 * {@link #toText(String)} on the way out so it lands in the same byte space.
 */
public final class Bytes {

    /** The charset used to move between raw bytes and wire strings. */
    public static final Charset WIRE = StandardCharsets.ISO_8859_1;

    private Bytes() {
    }

    public static String fromBytes(byte[] raw) {
        return new String(raw, WIRE);
    }

    public static byte[] toBytes(String wire) {
        return wire.getBytes(WIRE);
    }

    /** UTF-8 text (HTTP/JSON) -> wire string. */
    public static String fromText(String text) {
        return new String(text.getBytes(StandardCharsets.UTF_8), WIRE);
    }

    /** Wire string -> UTF-8 text (HTTP/JSON). */
    public static String toText(String wire) {
        return new String(wire.getBytes(WIRE), StandardCharsets.UTF_8);
    }
}
