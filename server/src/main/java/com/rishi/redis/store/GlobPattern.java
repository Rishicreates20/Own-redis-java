package com.rishi.redis.store;

/**
 * Redis-style glob matching, used by {@code KEYS}, {@code SCAN MATCH} and
 * {@code PSUBSCRIBE}. A direct port of the {@code stringmatchlen} algorithm:
 * supports {@code *}, {@code ?}, character classes {@code [abc]} / {@code [a-c]} /
 * {@code [^abc]}, and {@code \} escaping.
 *
 * <p>Deliberately not delegated to {@code java.util.regex}: translating glob to
 * regex has to escape every regex metacharacter in the pattern, and a mistake
 * there turns a key lookup into a catastrophic-backtracking hazard.
 */
public final class GlobPattern {

    private GlobPattern() {
    }

    public static boolean matches(String pattern, String text) {
        return matches(pattern, 0, text, 0);
    }

    private static boolean matches(String pattern, int p, String text, int t) {
        int pn = pattern.length();
        int tn = text.length();

        while (p < pn && t <= tn) {
            char pc = pattern.charAt(p);
            switch (pc) {
                case '*' -> {
                    // Collapse runs of '*' then try every possible split point.
                    while (p + 1 < pn && pattern.charAt(p + 1) == '*') {
                        p++;
                    }
                    if (p + 1 == pn) {
                        return true;
                    }
                    for (int i = t; i <= tn; i++) {
                        if (matches(pattern, p + 1, text, i)) {
                            return true;
                        }
                    }
                    return false;
                }
                case '?' -> {
                    if (t == tn) {
                        return false;
                    }
                    t++;
                    p++;
                }
                case '[' -> {
                    if (t == tn) {
                        return false;
                    }
                    int cursor = p + 1;
                    boolean negate = cursor < pn && pattern.charAt(cursor) == '^';
                    if (negate) {
                        cursor++;
                    }
                    boolean match = false;
                    while (true) {
                        if (cursor >= pn) {
                            break;
                        }
                        char c = pattern.charAt(cursor);
                        if (c == '\\' && cursor + 1 < pn) {
                            cursor++;
                            if (pattern.charAt(cursor) == text.charAt(t)) {
                                match = true;
                            }
                        } else if (c == ']') {
                            break;
                        } else if (cursor + 2 < pn && pattern.charAt(cursor + 1) == '-'
                                && pattern.charAt(cursor + 2) != ']') {
                            char low = c;
                            char high = pattern.charAt(cursor + 2);
                            if (low > high) {
                                char swap = low;
                                low = high;
                                high = swap;
                            }
                            char actual = text.charAt(t);
                            if (actual >= low && actual <= high) {
                                match = true;
                            }
                            cursor += 2;
                        } else if (c == text.charAt(t)) {
                            match = true;
                        }
                        cursor++;
                    }
                    if (negate) {
                        match = !match;
                    }
                    if (!match) {
                        return false;
                    }
                    // Skip past the closing bracket.
                    p = cursor < pn ? cursor + 1 : pn;
                    t++;
                }
                case '\\' -> {
                    if (p + 1 < pn) {
                        p++;
                    }
                    if (t == tn || pattern.charAt(p) != text.charAt(t)) {
                        return false;
                    }
                    t++;
                    p++;
                }
                default -> {
                    if (t == tn || pc != text.charAt(t)) {
                        return false;
                    }
                    t++;
                    p++;
                }
            }
            if (t == tn) {
                // Trailing '*' can still match the empty remainder.
                while (p < pn && pattern.charAt(p) == '*') {
                    p++;
                }
                break;
            }
        }
        return p == pn && t == tn;
    }
}
