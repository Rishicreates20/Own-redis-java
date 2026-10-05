package com.rishi.redis.command;

import com.rishi.redis.protocol.RespValue;

/**
 * One Redis command. Implementations are stateless singletons (usually method
 * references) held in the {@link CommandTable}.
 *
 * <p>Returning {@code null} means "no reply" — used by the few commands, such as
 * SUBSCRIBE, that write their own frames directly to the client.
 */
@FunctionalInterface
public interface CommandHandler {

    RespValue execute(CommandContext ctx);
}
