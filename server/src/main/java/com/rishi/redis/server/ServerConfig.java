package com.rishi.redis.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Runtime configuration, resolved from (in increasing precedence) built-in defaults,
 * an optional redis.conf-style file, and command-line flags.
 */
public record ServerConfig(
        String host,
        int port,
        boolean httpEnabled,
        int httpPort,
        String corsOrigin,
        int databases,
        String password,
        Path dir,
        String dbFilename,
        boolean loadSnapshotOnStart,
        boolean saveSnapshotOnShutdown,
        long expiryIntervalMillis,
        ExecutionModel executionModel) {

    /**
     * TD-3 — how a parsed command actually gets executed.
     *
     * <ul>
     *   <li>{@code INLINE} runs it on the Netty event loop. Every command implemented
     *       here is non-blocking and finishes in microseconds, so this is the lowest
     *       latency option and the default.</li>
     *   <li>{@code VIRTUAL_THREADS} hands each command to a virtual thread, serialised
     *       per connection so pipelined replies keep their order. This is the model
     *       blocking commands (BLPOP and friends) will need, and it keeps the event
     *       loop free under slow handlers.</li>
     * </ul>
     */
    public enum ExecutionModel {
        INLINE,
        VIRTUAL_THREADS
    }

    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_HTTP_PORT = 8080;

    public Path snapshotPath() {
        return dir.resolve(dbFilename);
    }

    public boolean requiresAuth() {
        return password != null && !password.isEmpty();
    }

    public static ServerConfig defaults() {
        return new ServerConfig(
                "0.0.0.0",
                DEFAULT_PORT,
                true,
                DEFAULT_HTTP_PORT,
                "*",
                16,
                null,
                Path.of("."),
                "dump.ordb",
                true,
                true,
                100L,
                ExecutionModel.INLINE);
    }

    public static final String USAGE = """
            Own-Redis-Java — a RESP-compatible in-memory store for the JVM

              java -jar own-redis-java.jar [options]

            Options
              --config <file>          load a redis.conf-style file first
              --host <addr>            bind address                    (default 0.0.0.0)
              --port <n>               RESP port                       (default 6379)
              --http-port <n>          REST bridge port for the Web CLI (default 8080)
              --no-http                disable the REST bridge
              --cors-origin <origin>   Access-Control-Allow-Origin      (default *)
              --databases <n>          number of SELECTable databases   (default 16)
              --requirepass <secret>   require AUTH before commands
              --dir <path>             snapshot directory               (default .)
              --dbfilename <name>      snapshot file name               (default dump.ordb)
              --no-load                do not load the snapshot at boot
              --no-save-on-shutdown    do not snapshot on clean shutdown
              --expiry-interval <ms>   active expiry cycle period       (default 100)
              --exec-model <mode>      inline | vthread                 (default inline)
              --help                   print this message
            """;

    /** @throws IllegalArgumentException on an unknown flag or an unparseable value */
    public static ServerConfig parse(String[] args) {
        ServerConfig config = defaults();

        // A --config file is applied first, wherever it appears, so flags always win.
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--config")) {
                config = applyFile(config, Path.of(args[i + 1]));
            }
        }

        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--config" -> i++; // already applied
                case "--no-http" -> config = config.withHttpEnabled(false);
                case "--no-load" -> config = config.withLoadSnapshotOnStart(false);
                case "--no-save-on-shutdown" -> config = config.withSaveSnapshotOnShutdown(false);
                case "--help", "-h" -> throw new HelpRequested();
                default -> {
                    if (!flag.startsWith("--")) {
                        throw new IllegalArgumentException("unexpected argument: " + flag);
                    }
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("missing value for " + flag);
                    }
                    config = apply(config, flag.substring(2), args[++i]);
                }
            }
        }
        return config;
    }

    /** Signals {@code --help}; the caller prints {@link #USAGE} and exits cleanly. */
    public static final class HelpRequested extends RuntimeException {
        public HelpRequested() {
            super(null, null, false, false);
        }
    }

    private static ServerConfig applyFile(ServerConfig config, Path file) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read config file " + file + ": " + e.getMessage());
        }
        for (String raw : lines) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int split = line.indexOf(' ');
            if (split < 0) {
                config = apply(config, line, "yes");
            } else {
                config = apply(config, line.substring(0, split).strip(),
                        stripQuotes(line.substring(split + 1).strip()));
            }
        }
        return config;
    }

    private static String stripQuotes(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static ServerConfig apply(ServerConfig config, String key, String value) {
        return switch (key.toLowerCase()) {
            case "host", "bind" -> config.withHost(value);
            case "port" -> config.withPort(parsePort(value, "port"));
            case "http-port", "httpport" -> config.withHttpPort(parsePort(value, "http-port"));
            case "http", "http-enabled" -> config.withHttpEnabled(parseBool(value));
            case "cors-origin" -> config.withCorsOrigin(value);
            case "databases" -> config.withDatabases(parseCount(value, "databases", 1, 1024));
            case "requirepass", "password" -> config.withPassword(value.isEmpty() ? null : value);
            case "dir" -> config.withDir(Path.of(value));
            case "dbfilename" -> config.withDbFilename(value);
            case "load-snapshot" -> config.withLoadSnapshotOnStart(parseBool(value));
            case "save-on-shutdown" -> config.withSaveSnapshotOnShutdown(parseBool(value));
            case "expiry-interval" -> config.withExpiryIntervalMillis(
                    parseCount(value, "expiry-interval", 1, 60_000));
            case "exec-model" -> config.withExecutionModel(parseExecutionModel(value));
            default -> throw new IllegalArgumentException("unknown option: " + key);
        };
    }

    private static ExecutionModel parseExecutionModel(String value) {
        return switch (value.toLowerCase()) {
            case "inline", "event-loop" -> ExecutionModel.INLINE;
            case "vthread", "virtual", "virtual-threads", "loom" -> ExecutionModel.VIRTUAL_THREADS;
            default -> throw new IllegalArgumentException("exec-model must be inline or vthread");
        };
    }

    private static boolean parseBool(String value) {
        return switch (value.toLowerCase()) {
            case "yes", "true", "1", "on" -> true;
            case "no", "false", "0", "off" -> false;
            default -> throw new IllegalArgumentException("expected yes/no, got: " + value);
        };
    }

    private static int parsePort(String value, String what) {
        return parseCount(value, what, 1, 65535);
    }

    private static int parseCount(String value, String what, int min, int max) {
        int parsed;
        try {
            parsed = Integer.parseInt(value.strip());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(what + " must be a number, got: " + value);
        }
        if (parsed < min || parsed > max) {
            throw new IllegalArgumentException(what + " must be between " + min + " and " + max);
        }
        return parsed;
    }

    // ---- copy helpers (records are immutable; these keep parse() readable) ----

    public ServerConfig withHost(String v) {
        return new ServerConfig(v, port, httpEnabled, httpPort, corsOrigin, databases, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withPort(int v) {
        return new ServerConfig(host, v, httpEnabled, httpPort, corsOrigin, databases, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withHttpEnabled(boolean v) {
        return new ServerConfig(host, port, v, httpPort, corsOrigin, databases, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withHttpPort(int v) {
        return new ServerConfig(host, port, httpEnabled, v, corsOrigin, databases, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withCorsOrigin(String v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, v, databases, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withDatabases(int v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, v, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withPassword(String v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, databases, v, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withDir(Path v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, databases, password, v,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withDbFilename(String v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, databases, password, dir,
                v, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withLoadSnapshotOnStart(boolean v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, databases, password, dir,
                dbFilename, v, saveSnapshotOnShutdown, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withSaveSnapshotOnShutdown(boolean v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, databases, password, dir,
                dbFilename, loadSnapshotOnStart, v, expiryIntervalMillis, executionModel);
    }

    public ServerConfig withExpiryIntervalMillis(long v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, databases, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, v, executionModel);
    }

    public ServerConfig withExecutionModel(ExecutionModel v) {
        return new ServerConfig(host, port, httpEnabled, httpPort, corsOrigin, databases, password, dir,
                dbFilename, loadSnapshotOnStart, saveSnapshotOnShutdown, expiryIntervalMillis, v);
    }
}
