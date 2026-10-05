package com.rishi.redis.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a raw command line into arguments the way {@code redis-cli} does
 * (a port of {@code sdssplitargs}): whitespace separates arguments, double
 * quotes group them and honour {@code \xNN}, {@code \n}, {@code \t}, {@code \r},
 * {@code \b}, {@code \a}, {@code \\} and {@code \"} escapes; single quotes group
 * literally and only honour an escaped single quote.
 *
 * <p>Used by the inline-command path of {@link RespDecoder} (telnet / netcat)
 * and by the HTTP bridge, which receives a whole command as one line of text.
 */
public final class CommandLineSplitter {

    private CommandLineSplitter() {
    }

    /**
     * @throws IllegalArgumentException if the line ends inside an open quote
     */
    public static List<String> split(String line) {
        List<String> out = new ArrayList<>();
        int i = 0;
        int n = line.length();
        while (i < n) {
            while (i < n && isSpace(line.charAt(i))) {
                i++;
            }
            if (i >= n) {
                break;
            }
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;
            boolean inSingleQuotes = false;
            boolean done = false;
            while (!done) {
                if (inQuotes) {
                    if (i >= n) {
                        throw new IllegalArgumentException("unbalanced quotes in request");
                    }
                    char c = line.charAt(i);
                    if (c == '\\' && i + 3 < n && line.charAt(i + 1) == 'x'
                            && isHex(line.charAt(i + 2)) && isHex(line.charAt(i + 3))) {
                        current.append((char) ((hex(line.charAt(i + 2)) << 4) | hex(line.charAt(i + 3))));
                        i += 4;
                    } else if (c == '\\' && i + 1 < n) {
                        current.append(unescape(line.charAt(i + 1)));
                        i += 2;
                    } else if (c == '"') {
                        // A closing quote must be followed by a separator.
                        if (i + 1 < n && !isSpace(line.charAt(i + 1))) {
                            throw new IllegalArgumentException("unbalanced quotes in request");
                        }
                        i++;
                        done = true;
                    } else {
                        current.append(c);
                        i++;
                    }
                } else if (inSingleQuotes) {
                    if (i >= n) {
                        throw new IllegalArgumentException("unbalanced quotes in request");
                    }
                    char c = line.charAt(i);
                    if (c == '\\' && i + 1 < n && line.charAt(i + 1) == '\'') {
                        current.append('\'');
                        i += 2;
                    } else if (c == '\'') {
                        if (i + 1 < n && !isSpace(line.charAt(i + 1))) {
                            throw new IllegalArgumentException("unbalanced quotes in request");
                        }
                        i++;
                        done = true;
                    } else {
                        current.append(c);
                        i++;
                    }
                } else if (i >= n) {
                    done = true;
                } else {
                    char c = line.charAt(i);
                    if (isSpace(c)) {
                        done = true;
                    } else if (c == '"') {
                        inQuotes = true;
                        i++;
                    } else if (c == '\'') {
                        inSingleQuotes = true;
                        i++;
                    } else {
                        current.append(c);
                        i++;
                    }
                }
            }
            out.add(current.toString());
        }
        return out;
    }

    private static char unescape(char c) {
        return switch (c) {
            case 'n' -> '\n';
            case 'r' -> '\r';
            case 't' -> '\t';
            case 'b' -> '\b';
            case 'a' -> (char) 7;
            default -> c;
        };
    }

    private static boolean isSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 11 || c == '\f';
    }

    private static boolean isHex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private static int hex(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        return (Character.toLowerCase(c) - 'a') + 10;
    }
}
