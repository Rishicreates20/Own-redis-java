package com.rishi.redis.command.commands;

import com.rishi.redis.RedisException;
import com.rishi.redis.command.CommandContext;
import com.rishi.redis.command.CommandSpec;
import com.rishi.redis.command.CommandSpec.Flag;
import com.rishi.redis.command.CommandTable;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.server.ServerContext;
import com.rishi.redis.store.Database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Administrative commands: introspection, flushing, and the snapshot triggers.
 */
public final class ServerCommands {

    private ServerCommands() {
    }

    public static void register(CommandTable table) {
        table.register("command", -1, ServerCommands::command, Flag.NO_LOCK, Flag.NO_AUTH);
        table.register("info", -1, ServerCommands::info, Flag.NO_LOCK);
        table.register("dbsize", 1, ServerCommands::dbsize);
        table.register("flushdb", -1, ServerCommands::flushdb, Flag.WRITE);
        table.register("flushall", -1, ServerCommands::flushall, Flag.WRITE);
        table.register("save", 1, ServerCommands::save);
        table.register("bgsave", -1, ServerCommands::bgsave);
        table.register("lastsave", 1, ServerCommands::lastsave, Flag.NO_LOCK);
        table.register("config", -2, ServerCommands::config, Flag.NO_LOCK);
        table.register("time", 1, ServerCommands::time, Flag.NO_LOCK);
        table.register("shutdown", -1, ServerCommands::shutdown, Flag.NO_LOCK);
    }

    /**
     * {@code redis-cli} issues COMMAND DOCS on connect. Returning an empty reply is a
     * valid "I have no metadata for you" answer and keeps the CLI happy; COMMAND COUNT
     * and a bare COMMAND report the real table.
     */
    static RespValue command(CommandContext ctx) {
        if (ctx.argc() == 1) {
            List<RespValue> commands = new ArrayList<>();
            for (CommandSpec spec : ctx.server().commands().all()) {
                commands.add(RespValue.array(List.of(
                        RespValue.bulk(spec.name()),
                        RespValue.integer(spec.arity()),
                        RespValue.emptyArray(),
                        RespValue.ZERO,
                        RespValue.ZERO,
                        RespValue.ZERO)));
            }
            return RespValue.array(commands);
        }
        return switch (ctx.option(1)) {
            case "COUNT" -> RespValue.integer(ctx.server().commands().size());
            case "DOCS" -> RespValue.emptyArray();
            case "INFO" -> {
                List<RespValue> replies = new ArrayList<>();
                for (int i = 2; i < ctx.argc(); i++) {
                    CommandSpec spec = ctx.server().commands().lookup(ctx.arg(i));
                    replies.add(spec == null ? RespValue.NIL_ARRAY : RespValue.array(List.of(
                            RespValue.bulk(spec.name()),
                            RespValue.integer(spec.arity()),
                            RespValue.emptyArray(),
                            RespValue.ZERO,
                            RespValue.ZERO,
                            RespValue.ZERO)));
                }
                yield RespValue.array(replies);
            }
            default -> RespValue.emptyArray();
        };
    }

    static RespValue info(CommandContext ctx) {
        String section = ctx.argc() > 1 ? ctx.option(1) : "ALL";
        ServerContext server = ctx.server();
        StringBuilder out = new StringBuilder();

        if (matches(section, "SERVER")) {
            out.append("# Server\r\n")
                    .append("redis_version:7.0.0-compatible\r\n")
                    .append("server_name:").append(ConnectionCommands.SERVER_NAME).append("\r\n")
                    .append("server_version:").append(ConnectionCommands.SERVER_VERSION).append("\r\n")
                    .append("java_version:").append(System.getProperty("java.version")).append("\r\n")
                    .append("execution_model:")
                    .append(server.config().executionModel().name().toLowerCase(Locale.ROOT)).append("\r\n")
                    .append("os:").append(System.getProperty("os.name")).append("\r\n")
                    .append("process_id:").append(ProcessHandle.current().pid()).append("\r\n")
                    .append("tcp_port:").append(server.config().port()).append("\r\n")
                    .append("http_port:")
                    .append(server.config().httpEnabled() ? server.config().httpPort() : 0).append("\r\n")
                    .append("uptime_in_seconds:").append(server.uptimeSeconds()).append("\r\n\r\n");
        }
        if (matches(section, "CLIENTS")) {
            out.append("# Clients\r\n")
                    .append("connected_clients:").append(server.connectedClients()).append("\r\n")
                    .append("total_connections_received:").append(server.connectionsReceived()).append("\r\n\r\n");
        }
        if (matches(section, "MEMORY")) {
            Runtime runtime = Runtime.getRuntime();
            long used = runtime.totalMemory() - runtime.freeMemory();
            out.append("# Memory\r\n")
                    .append("used_memory:").append(used).append("\r\n")
                    .append("used_memory_human:").append(used / (1024 * 1024)).append("M\r\n")
                    .append("maxmemory:").append(runtime.maxMemory()).append("\r\n")
                    .append("gc_collector:").append(System.getProperty("java.vm.name")).append("\r\n\r\n");
        }
        if (matches(section, "PERSISTENCE")) {
            out.append("# Persistence\r\n")
                    .append("rdb_bgsave_in_progress:").append(server.snapshots().isSaving() ? 1 : 0).append("\r\n")
                    .append("rdb_last_save_time:").append(server.snapshots().lastSaveTimeSeconds()).append("\r\n")
                    .append("rdb_changes_since_last_save:").append(server.snapshots().changesSinceSave()).append("\r\n")
                    .append("snapshot_file:").append(server.snapshots().path()).append("\r\n\r\n");
        }
        if (matches(section, "STATS")) {
            out.append("# Stats\r\n")
                    .append("total_commands_processed:").append(server.commandsProcessed()).append("\r\n")
                    .append("keyspace_hits:").append(server.keyspaceHits()).append("\r\n")
                    .append("keyspace_misses:").append(server.keyspaceMisses()).append("\r\n")
                    .append("expired_keys:").append(server.expiration().expiredKeys()).append("\r\n")
                    .append("active_expiry_cycles:").append(server.expiration().cycles()).append("\r\n")
                    .append("pubsub_channels:").append(server.pubsub().activeChannels(null).size()).append("\r\n")
                    .append("pubsub_patterns:").append(server.pubsub().patternCount()).append("\r\n\r\n");
        }
        if (matches(section, "KEYSPACE")) {
            out.append("# Keyspace\r\n");
            for (Database database : server.databases()) {
                int size = database.size();
                if (size > 0) {
                    out.append("db").append(database.index()).append(":keys=").append(size)
                            .append(",expires=").append(database.rawExpires().size()).append("\r\n");
                }
            }
            out.append("\r\n");
        }
        return RespValue.bulk(out.toString());
    }

    private static boolean matches(String requested, String section) {
        return requested.equals("ALL") || requested.equals("DEFAULT") || requested.equals(section);
    }

    static RespValue dbsize(CommandContext ctx) {
        return RespValue.integer(ctx.db().size());
    }

    static RespValue flushdb(CommandContext ctx) {
        ctx.db().flush();
        return RespValue.OK;
    }

    static RespValue flushall(CommandContext ctx) {
        for (Database database : ctx.server().databases()) {
            database.flush();
        }
        return RespValue.OK;
    }

    static RespValue save(CommandContext ctx) {
        try {
            long written = ctx.server().snapshots().save();
            System.out.println("[persist] SAVE wrote " + written + " keys");
            return RespValue.OK;
        } catch (IOException e) {
            throw new RedisException("ERR snapshot failed: " + e.getMessage());
        }
    }

    static RespValue bgsave(CommandContext ctx) {
        if (!ctx.server().snapshots().backgroundSave()) {
            throw new RedisException("ERR Background save already in progress");
        }
        return RespValue.simple("Background saving started");
    }

    static RespValue lastsave(CommandContext ctx) {
        return RespValue.integer(ctx.server().snapshots().lastSaveTimeSeconds());
    }

    /** A read-only subset of CONFIG GET, plus a CONFIG SET that refuses politely. */
    static RespValue config(CommandContext ctx) {
        String subcommand = ctx.option(1);
        if (subcommand.equals("GET")) {
            if (ctx.argc() < 3) {
                throw RedisException.wrongArgs("config|get");
            }
            List<RespValue> out = new ArrayList<>();
            for (int i = 2; i < ctx.argc(); i++) {
                String pattern = ctx.arg(i);
                for (String[] entry : settings(ctx.server())) {
                    if (com.rishi.redis.store.GlobPattern.matches(pattern, entry[0])) {
                        out.add(RespValue.bulk(entry[0]));
                        out.add(RespValue.bulk(entry[1]));
                    }
                }
            }
            return RespValue.array(out);
        }
        if (subcommand.equals("RESETSTAT")) {
            return RespValue.OK;
        }
        throw new RedisException("ERR CONFIG " + ctx.arg(1)
                + " is not supported; restart with the matching command-line flag instead");
    }

    private static List<String[]> settings(ServerContext server) {
        return List.of(
                new String[]{"port", Integer.toString(server.config().port())},
                new String[]{"bind", server.config().host()},
                new String[]{"databases", Integer.toString(server.config().databases())},
                new String[]{"dir", server.config().dir().toAbsolutePath().toString()},
                new String[]{"dbfilename", server.config().dbFilename()},
                new String[]{"requirepass", server.config().requiresAuth() ? "(set)" : ""},
                new String[]{"appendonly", "no"},
                new String[]{"maxmemory", "0"},
                new String[]{"http-port", Integer.toString(server.config().httpPort())});
    }

    static RespValue time(CommandContext ctx) {
        long now = System.currentTimeMillis();
        return RespValue.array(List.of(
                RespValue.bulk(Long.toString(now / 1000L)),
                RespValue.bulk(Long.toString((now % 1000L) * 1000L))));
    }

    static RespValue shutdown(CommandContext ctx) {
        boolean save = ctx.server().config().saveSnapshotOnShutdown();
        for (int i = 1; i < ctx.argc(); i++) {
            switch (ctx.option(i)) {
                case "NOSAVE" -> save = false;
                case "SAVE" -> save = true;
                default -> throw RedisException.syntaxError();
            }
        }
        System.out.println("[server] SHUTDOWN requested by " + ctx.session().id()
                + " (save=" + save + ")");
        if (save) {
            try {
                ctx.server().snapshots().save();
            } catch (IOException e) {
                throw new RedisException("ERR Errors trying to SHUTDOWN: " + e.getMessage());
            }
        }
        // Exit on another thread so this connection's reply is never expected: Redis
        // clients treat a closed socket as the successful outcome of SHUTDOWN.
        Thread exit = new Thread(() -> Runtime.getRuntime().exit(0), "redis-shutdown");
        exit.setDaemon(false);
        exit.start();
        return null;
    }
}
