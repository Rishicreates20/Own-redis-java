package com.rishi.redis;

import com.rishi.redis.server.HttpBridgeServer;
import com.rishi.redis.server.RedisServer;
import com.rishi.redis.server.ServerConfig;
import com.rishi.redis.server.ServerContext;

import java.util.Locale;

/**
 * Entry point: resolve configuration, build the server, open both front doors
 * (RESP for real Redis clients, HTTP for the Web CLI), and wait.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws InterruptedException {
        ServerConfig config;
        try {
            config = ServerConfig.parse(args);
        } catch (ServerConfig.HelpRequested help) {
            System.out.println(ServerConfig.USAGE);
            return;
        } catch (IllegalArgumentException bad) {
            System.err.println("error: " + bad.getMessage());
            System.err.println();
            System.err.println(ServerConfig.USAGE);
            Runtime.getRuntime().exit(2);
            return;
        }

        ServerContext context = new ServerContext(config);
        context.start();

        RedisServer resp = new RedisServer(context);
        HttpBridgeServer http = config.httpEnabled() ? new HttpBridgeServer(context) : null;

        resp.start();
        if (http != null) {
            http.start();
        }
        printBanner(context);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("[server] shutting down...");
            resp.stop();
            if (http != null) {
                http.stop();
            }
            context.shutdown();
        }, "redis-shutdown-hook"));

        resp.awaitShutdown();
    }

    private static void printBanner(ServerContext context) {
        ServerConfig config = context.config();
        String host = config.host().equals("0.0.0.0") ? "localhost" : config.host();
        System.out.println("""

                  ___                   ___         _ _
                 / _ \\__ __ ___ _      | _ \\___ ___| (_)___
                | (_) \\ V  V / _ \\_    |   / -_) _-| | (_-<
                 \\___/ \\_/\\_/_//_(_)   |_|_\\___\\___|_|_/__/   Java edition
                """);
        System.out.println("  RESP        redis-cli -p " + config.port());
        if (config.httpEnabled()) {
            System.out.println("  Web CLI     http://" + host + ":" + config.httpPort() + "/api/command");
            System.out.println("  Health      http://" + host + ":" + config.httpPort() + "/api/health");
        } else {
            System.out.println("  Web CLI     disabled (--no-http)");
        }
        System.out.println("  Databases   " + context.databaseCount()
                + "   Commands " + context.commands().size()
                + "   Exec " + config.executionModel().name().toLowerCase(Locale.ROOT));
        System.out.println("  Snapshot    " + config.snapshotPath().toAbsolutePath());
        System.out.println("  Auth        " + (config.requiresAuth() ? "required" : "disabled"));
        System.out.println("  Java        " + System.getProperty("java.version")
                + " on " + System.getProperty("os.name"));
        System.out.println();
        System.out.println("  ready.");
        System.out.println();
    }
}
