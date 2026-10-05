package com.rishi.redis.server;

import com.rishi.redis.command.CommandTable;
import com.rishi.redis.persist.SnapshotManager;
import com.rishi.redis.pubsub.PubSubRegistry;
import com.rishi.redis.store.Database;
import com.rishi.redis.store.ExpirationCycle;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Everything one server instance owns: the databases, the command table, the Pub/Sub
 * registry, the snapshot writer, the expiry cycle, and the live sessions. Handlers
 * receive it rather than reaching for statics, which is what lets a test spin up a
 * whole server in-process without binding a port.
 */
public final class ServerContext {

    private final ServerConfig config;
    private final Database[] databases;
    private final CommandTable commands;
    private final PubSubRegistry pubsub;
    private final SnapshotManager snapshots;
    private final ExpirationCycle expiration;

    private final long startTimeMillis = System.currentTimeMillis();
    private final AtomicLong connectionsReceived = new AtomicLong();
    private final AtomicLong commandsProcessed = new AtomicLong();
    private final AtomicLong keyspaceHits = new AtomicLong();
    private final AtomicLong keyspaceMisses = new AtomicLong();

    private final Set<ClientSession> sessions = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, ClientSession> httpSessions = new ConcurrentHashMap<>();

    /** TD-3 — non-null only when running the virtual-thread execution model. */
    private final ExecutorService commandExecutor;

    private volatile boolean shuttingDown;

    public ServerContext(ServerConfig config) {
        this.config = config;
        this.databases = new Database[config.databases()];
        for (int i = 0; i < databases.length; i++) {
            databases[i] = new Database(i);
        }
        this.commands = CommandTable.standard();
        this.pubsub = new PubSubRegistry();
        this.snapshots = new SnapshotManager(config.snapshotPath(), databases);
        this.expiration = new ExpirationCycle(databases, config.expiryIntervalMillis());
        this.commandExecutor = config.executionModel() == ServerConfig.ExecutionModel.VIRTUAL_THREADS
                ? Executors.newVirtualThreadPerTaskExecutor()
                : null;
    }

    public ServerConfig config() {
        return config;
    }

    public Database database(int index) {
        return databases[index];
    }

    public Database[] databases() {
        return databases;
    }

    public int databaseCount() {
        return databases.length;
    }

    public CommandTable commands() {
        return commands;
    }

    public PubSubRegistry pubsub() {
        return pubsub;
    }

    public SnapshotManager snapshots() {
        return snapshots;
    }

    public ExpirationCycle expiration() {
        return expiration;
    }

    public ExecutorService commandExecutor() {
        return commandExecutor;
    }

    public long startTimeMillis() {
        return startTimeMillis;
    }

    public long uptimeSeconds() {
        return (System.currentTimeMillis() - startTimeMillis) / 1000L;
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    // -------------------------------------------------------------- sessions

    public ClientSession openSession(ClientOutput output, String id) {
        ClientSession session = new ClientSession(this, output, id);
        sessions.add(session);
        connectionsReceived.incrementAndGet();
        return session;
    }

    public void closeSession(ClientSession session) {
        pubsub.removeAll(session);
        sessions.remove(session);
        httpSessions.remove(session.id(), session);
    }

    public Collection<ClientSession> sessions() {
        return sessions;
    }

    public int connectedClients() {
        return sessions.size();
    }

    /**
     * The HTTP bridge is stateless per request, but MULTI, SELECT and AUTH are not, so
     * each browser tab keeps a session keyed by its own id. Idle sessions are reaped
     * so a long-lived server cannot accumulate them without bound.
     */
    public ClientSession httpSession(String id, ClientOutput output) {
        ClientSession existing = httpSessions.get(id);
        if (existing != null) {
            existing.touch();
            return existing;
        }
        reapIdleHttpSessions();
        ClientSession created = openSession(output, id);
        ClientSession raced = httpSessions.putIfAbsent(id, created);
        if (raced != null) {
            closeSession(created);
            return raced;
        }
        return created;
    }

    private void reapIdleHttpSessions() {
        if (httpSessions.size() < 512) {
            return;
        }
        long cutoff = System.currentTimeMillis() - 30 * 60_000L;
        for (ClientSession session : httpSessions.values()) {
            if (session.lastAccess() < cutoff) {
                closeSession(session);
            }
        }
    }

    // ----------------------------------------------------------------- stats

    public void recordCommand() {
        commandsProcessed.incrementAndGet();
    }

    public void recordHit() {
        keyspaceHits.incrementAndGet();
    }

    public void recordMiss() {
        keyspaceMisses.incrementAndGet();
    }

    public long connectionsReceived() {
        return connectionsReceived.get();
    }

    public long commandsProcessed() {
        return commandsProcessed.get();
    }

    public long keyspaceHits() {
        return keyspaceHits.get();
    }

    public long keyspaceMisses() {
        return keyspaceMisses.get();
    }

    // -------------------------------------------------------------- lifecycle

    public void start() {
        if (config.loadSnapshotOnStart()) {
            try {
                long restored = snapshots.load();
                if (restored > 0) {
                    System.out.println("[persist] restored " + restored + " keys from " + snapshots.path());
                }
            } catch (IOException e) {
                System.err.println("[persist] could not load snapshot: " + e.getMessage());
            }
        }
        expiration.start();
    }

    public void shutdown() {
        shuttingDown = true;
        expiration.stop();
        if (commandExecutor != null) {
            commandExecutor.shutdown();
        }
        if (config.saveSnapshotOnShutdown()) {
            try {
                long written = snapshots.save();
                System.out.println("[persist] snapshot written on shutdown: " + written + " keys");
            } catch (IOException e) {
                System.err.println("[persist] shutdown snapshot failed: " + e.getMessage());
            }
        }
    }
}
