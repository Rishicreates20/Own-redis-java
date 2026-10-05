package com.rishi.redis.protocol;

import java.util.List;

/**
 * One decoded client request: the command name followed by its arguments, all as
 * wire strings. Emitted by {@link RespDecoder}, consumed by the command dispatcher.
 */
public record RespRequest(List<String> args) {

    public boolean isEmpty() {
        return args.isEmpty();
    }

    public String name() {
        return args.get(0);
    }

    @Override
    public String toString() {
        return String.join(" ", args);
    }
}
