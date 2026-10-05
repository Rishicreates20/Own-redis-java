package com.rishi.redis.command;

import com.rishi.redis.command.commands.ConnectionCommands;
import com.rishi.redis.command.commands.GenericCommands;
import com.rishi.redis.command.commands.HashCommands;
import com.rishi.redis.command.commands.ListCommands;
import com.rishi.redis.command.commands.PubSubCommands;
import com.rishi.redis.command.commands.ServerCommands;
import com.rishi.redis.command.commands.SetCommands;
import com.rishi.redis.command.commands.StringCommands;
import com.rishi.redis.command.commands.TransactionCommands;
import com.rishi.redis.command.commands.ZSetCommands;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * FR-2 — the dispatch table.
 *
 * <p>The first element of the request is looked up here in O(1) and the resulting
 * singleton handler is invoked with the connection context. Adding a command is one
 * {@code register(...)} line; nothing else in the pipeline changes.
 */
public final class CommandTable {

    private final Map<String, CommandSpec> byName = new HashMap<>(256);

    public void register(String name, int arity, CommandHandler handler, CommandSpec.Flag... flags) {
        Set<CommandSpec.Flag> flagSet = flags.length == 0
                ? EnumSet.noneOf(CommandSpec.Flag.class)
                : EnumSet.copyOf(java.util.Arrays.asList(flags));
        String key = name.toLowerCase(Locale.ROOT);
        if (byName.putIfAbsent(key, new CommandSpec(key, arity, flagSet, handler)) != null) {
            throw new IllegalStateException("duplicate command registration: " + name);
        }
    }

    /** @return the spec, or {@code null} when the command is unknown */
    public CommandSpec lookup(String name) {
        return byName.get(name.toLowerCase(Locale.ROOT));
    }

    public Collection<CommandSpec> all() {
        return new TreeMap<>(byName).values();
    }

    public int size() {
        return byName.size();
    }

    /** The v1.0 command surface from the PRD. */
    public static CommandTable standard() {
        CommandTable table = new CommandTable();
        ConnectionCommands.register(table);
        ServerCommands.register(table);
        StringCommands.register(table);
        GenericCommands.register(table);
        HashCommands.register(table);
        ListCommands.register(table);
        SetCommands.register(table);
        ZSetCommands.register(table);
        TransactionCommands.register(table);
        PubSubCommands.register(table);
        return table;
    }
}
