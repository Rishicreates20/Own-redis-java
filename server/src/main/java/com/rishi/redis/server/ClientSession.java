package com.rishi.redis.server;

import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.store.Database;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-connection state: which database is selected, whether the client has
 * authenticated, an in-flight MULTI queue with its watched keys, and the set of
 * Pub/Sub channels the connection is listening on.
 *
 * <p>Commands from one connection are always executed one at a time (the event loop
 * or the per-connection serial executor guarantees it), so the transaction fields are
 * plain fields rather than concurrent ones. The subscription sets are concurrent
 * because the Pub/Sub registry reads them from publisher threads.
 */
public final class ClientSession {

    /** A key WATCHed at a particular version; EXEC aborts if the version moved. */
    public record WatchedKey(int database, String key, long version) {
    }

    private final ServerContext server;
    private final ClientOutput output;
    private final String id;
    private final long createdAt = System.currentTimeMillis();

    private volatile int databaseIndex;
    private volatile boolean authenticated;
    private volatile long lastAccess = System.currentTimeMillis();
    private volatile String name = "";
    /** Set by QUIT: the transport closes the channel once the reply has been flushed. */
    private volatile boolean closeAfterReply;

    private boolean inMulti;
    private boolean transactionAborted;
    private final List<List<String>> queuedCommands = new ArrayList<>();
    private final List<WatchedKey> watchedKeys = new ArrayList<>();

    private final Set<String> channels = ConcurrentHashMap.newKeySet();
    private final Set<String> patterns = ConcurrentHashMap.newKeySet();

    public ClientSession(ServerContext server, ClientOutput output, String id) {
        this.server = server;
        this.output = output;
        this.id = id;
        this.authenticated = !server.config().requiresAuth();
    }

    public ServerContext server() {
        return server;
    }

    public ClientOutput output() {
        return output;
    }

    public String id() {
        return id;
    }

    public long createdAt() {
        return createdAt;
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void touch() {
        lastAccess = System.currentTimeMillis();
    }

    public long lastAccess() {
        return lastAccess;
    }

    public void push(RespValue value) {
        output.push(value);
    }

    public boolean closeAfterReply() {
        return closeAfterReply;
    }

    public void requestClose() {
        this.closeAfterReply = true;
    }

    // ------------------------------------------------------------ databases

    public int databaseIndex() {
        return databaseIndex;
    }

    public Database database() {
        return server.database(databaseIndex);
    }

    public void select(int index) {
        this.databaseIndex = index;
    }

    // ----------------------------------------------------------------- auth

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    // --------------------------------------------------------- transactions

    public boolean inMulti() {
        return inMulti;
    }

    public void beginMulti() {
        inMulti = true;
        transactionAborted = false;
        queuedCommands.clear();
    }

    public void queue(List<String> command) {
        queuedCommands.add(command);
    }

    public List<List<String>> queuedCommands() {
        return queuedCommands;
    }

    /** Marks the transaction poisoned: EXEC must refuse to run it (EXECABORT). */
    public void abortTransaction() {
        transactionAborted = true;
    }

    public boolean isTransactionAborted() {
        return transactionAborted;
    }

    public void endMulti() {
        inMulti = false;
        transactionAborted = false;
        queuedCommands.clear();
        unwatchAll();
    }

    public void watch(String key) {
        Database database = database();
        watchedKeys.add(new WatchedKey(databaseIndex, key, database.version(key)));
    }

    public List<WatchedKey> watchedKeys() {
        return watchedKeys;
    }

    public void unwatchAll() {
        watchedKeys.clear();
    }

    /** True when every watched key still carries the version it had at WATCH time. */
    public boolean watchesIntact() {
        for (WatchedKey watched : watchedKeys) {
            Database database = server.database(watched.database());
            if (database.version(watched.key()) != watched.version()) {
                return false;
            }
        }
        return true;
    }

    // -------------------------------------------------------------- pub/sub

    public Set<String> channels() {
        return channels;
    }

    public Set<String> patterns() {
        return patterns;
    }

    public int subscriptionCount() {
        return channels.size() + patterns.size();
    }

    /**
     * A connection that holds any subscription is in "subscriber mode": Redis only
     * accepts a small command set until the last subscription is dropped.
     */
    public boolean inSubscriberMode() {
        return subscriptionCount() > 0;
    }

    @Override
    public String toString() {
        return "session[" + id + " db=" + databaseIndex + "]";
    }
}
