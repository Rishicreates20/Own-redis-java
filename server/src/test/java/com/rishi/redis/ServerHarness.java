package com.rishi.redis;

import com.rishi.redis.command.CommandDispatcher;
import com.rishi.redis.protocol.CommandLineSplitter;
import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.server.ClientOutput;
import com.rishi.redis.server.ClientSession;
import com.rishi.redis.server.ReplyFormatter;
import com.rishi.redis.server.ServerConfig;
import com.rishi.redis.server.ServerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives a whole server in-process — no sockets, no ports — so tests exercise the
 * real dispatcher, keyspace and locking rather than a stand-in.
 */
public final class ServerHarness {

    private final ServerContext context;
    private final ClientSession session;
    private final List<RespValue> pushed = new ArrayList<>();

    public ServerHarness() {
        this(ServerConfig.defaults().withLoadSnapshotOnStart(false).withSaveSnapshotOnShutdown(false));
    }

    public ServerHarness(ServerConfig config) {
        this.context = new ServerContext(config);
        this.session = context.openSession(new RecordingOutput(), "test-1");
    }

    public ServerContext context() {
        return context;
    }

    public ClientSession session() {
        return session;
    }

    /** Opens a second connection against the same server, for concurrency tests. */
    public ClientSession newSession(String id) {
        return context.openSession(ClientOutput.DISCARDING, id);
    }

    public RespValue run(String commandLine) {
        return run(session, commandLine);
    }

    public RespValue run(ClientSession target, String commandLine) {
        return CommandDispatcher.dispatch(context, target, CommandLineSplitter.split(commandLine));
    }

    /** The reply as redis-cli would print it. */
    public String text(String commandLine) {
        RespValue reply = run(commandLine);
        return reply == null ? "" : ReplyFormatter.toText(reply);
    }

    /** Frames pushed to this session out of band (Pub/Sub). */
    public List<RespValue> pushed() {
        return pushed;
    }

    private final class RecordingOutput implements ClientOutput {
        @Override
        public void push(RespValue value) {
            pushed.add(value);
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public String remoteAddress() {
            return "test";
        }
    }
}
