package com.rishi.redis.server;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A deliberately tiny JSON reader/writer for the HTTP bridge.
 *
 * <p>The bridge exchanges flat objects of scalars ({@code {"command": "SET k v"}}), so
 * pulling in Jackson to parse them would mean adding a dependency — and a
 * deserialisation attack surface — for about eighty lines of work. Nested values are
 * skipped rather than modelled: nothing in the protocol uses them.
 */
public final class Json {

    private Json() {
    }

    /** @throws IllegalArgumentException if the text is not a JSON object */
    public static Map<String, String> parseObject(String text) {
        Parser parser = new Parser(text);
        parser.skipWhitespace();
        parser.expect('{');
        Map<String, String> fields = new LinkedHashMap<>();
        parser.skipWhitespace();
        if (parser.peek() == '}') {
            parser.next();
            return fields;
        }
        while (true) {
            parser.skipWhitespace();
            String key = parser.readString();
            parser.skipWhitespace();
            parser.expect(':');
            parser.skipWhitespace();
            fields.put(key, parser.readValue());
            parser.skipWhitespace();
            char c = parser.next();
            if (c == '}') {
                return fields;
            }
            if (c != ',') {
                throw new IllegalArgumentException("expected ',' or '}' at offset " + parser.position());
            }
        }
    }

    public static String escape(String text) {
        StringBuilder out = new StringBuilder(text.length() + 16);
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    /** Builds {@code {"a":"1","b":"2"}} from alternating key/value pairs. */
    public static String object(Object... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("expected key/value pairs");
        }
        StringBuilder out = new StringBuilder("{");
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            if (i > 0) {
                out.append(',');
            }
            out.append('"').append(escape(String.valueOf(keyValuePairs[i]))).append("\":");
            Object value = keyValuePairs[i + 1];
            if (value instanceof Number || value instanceof Boolean) {
                out.append(value);
            } else if (value == null) {
                out.append("null");
            } else {
                out.append('"').append(escape(String.valueOf(value))).append('"');
            }
        }
        return out.append('}').toString();
    }

    private static final class Parser {
        private final String text;
        private int index;

        Parser(String text) {
            this.text = text;
        }

        int position() {
            return index;
        }

        void skipWhitespace() {
            while (index < text.length() && Character.isWhitespace(text.charAt(index))) {
                index++;
            }
        }

        char peek() {
            if (index >= text.length()) {
                throw new IllegalArgumentException("unexpected end of JSON");
            }
            return text.charAt(index);
        }

        char next() {
            char c = peek();
            index++;
            return c;
        }

        void expect(char expected) {
            char actual = next();
            if (actual != expected) {
                throw new IllegalArgumentException(
                        "expected '" + expected + "' but found '" + actual + "' at offset " + (index - 1));
            }
        }

        String readString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return out.toString();
                }
                if (c != '\\') {
                    out.append(c);
                    continue;
                }
                char escape = next();
                switch (escape) {
                    case '"' -> out.append('"');
                    case '\\' -> out.append('\\');
                    case '/' -> out.append('/');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'u' -> {
                        if (index + 4 > text.length()) {
                            throw new IllegalArgumentException("truncated unicode escape");
                        }
                        out.append((char) Integer.parseInt(text.substring(index, index + 4), 16));
                        index += 4;
                    }
                    default -> throw new IllegalArgumentException("bad escape: \\" + escape);
                }
            }
        }

        /** Scalars are returned as text; objects and arrays are skipped and reported as null. */
        String readValue() {
            char c = peek();
            if (c == '"') {
                return readString();
            }
            if (c == '{' || c == '[') {
                skipContainer();
                return null;
            }
            int start = index;
            while (index < text.length() && ",}] \t\r\n".indexOf(text.charAt(index)) < 0) {
                index++;
            }
            String literal = text.substring(start, index);
            return literal.equals("null") ? null : literal;
        }

        private void skipContainer() {
            int depth = 0;
            do {
                char c = next();
                if (c == '"') {
                    index--;
                    readString();
                } else if (c == '{' || c == '[') {
                    depth++;
                } else if (c == '}' || c == ']') {
                    depth--;
                }
            } while (depth > 0);
        }
    }
}
