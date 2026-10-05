package com.rishi.redis.server;

import com.rishi.redis.protocol.Bytes;
import com.rishi.redis.protocol.RespValue;

import java.util.List;

/**
 * Renders a {@link RespValue} the way {@code redis-cli} prints it, so the Web CLI's
 * terminal pane shows {@code (integer) 3} and {@code 1) "a"} rather than raw protocol.
 *
 * <p>Wire strings are converted back to UTF-8 text here — this is the boundary where
 * the byte-preserving representation used inside the server meets JSON, which must be
 * valid Unicode.
 */
public final class ReplyFormatter {

    private ReplyFormatter() {
    }

    public static String toText(RespValue value) {
        StringBuilder out = new StringBuilder();
        render(value, out, "");
        return out.toString();
    }

    public static boolean isError(RespValue value) {
        return value instanceof RespValue.Error;
    }

    private static void render(RespValue value, StringBuilder out, String indent) {
        switch (value) {
            case RespValue.SimpleString s -> out.append(Bytes.toText(s.value()));
            case RespValue.Error e -> out.append("(error) ").append(Bytes.toText(e.message()));
            case RespValue.Integer64 i -> out.append("(integer) ").append(i.value());
            case RespValue.BulkString b -> {
                if (b.value() == null) {
                    out.append("(nil)");
                } else {
                    out.append(Bytes.toText(b.value()));
                }
            }
            case RespValue.Array a -> {
                List<RespValue> items = a.items();
                if (items == null) {
                    out.append("(nil)");
                    return;
                }
                if (items.isEmpty()) {
                    out.append("(empty array)");
                    return;
                }
                // Redis-cli style: right-aligned index, nested levels indented under it.
                int width = Integer.toString(items.size()).length();
                for (int i = 0; i < items.size(); i++) {
                    if (i > 0) {
                        out.append('\n').append(indent);
                    }
                    String prefix = String.format("%" + width + "d) ", i + 1);
                    out.append(prefix);
                    render(items.get(i), out, indent + " ".repeat(prefix.length()));
                }
            }
        }
    }
}
