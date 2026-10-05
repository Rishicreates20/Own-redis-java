package com.rishi.redis.server;

import com.rishi.redis.command.CommandDispatcher;
import com.rishi.redis.protocol.RespProtocolException;
import com.rishi.redis.protocol.RespRequest;
import com.rishi.redis.protocol.RespValue;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The per-connection tail of the RESP pipeline: decoded request in, reply out.
 *
 * <p>Under the default {@code INLINE} execution model the command runs on the event
 * loop and replies are written without a flush, then flushed once per read — so a
 * pipelined batch of a thousand commands costs one syscall rather than a thousand.
 * Under {@code VIRTUAL_THREADS} the command is handed to a per-connection
 * {@link SerialExecutor}, which keeps ordering while freeing the event loop.
 */
public final class RespServerHandler extends SimpleChannelInboundHandler<RespRequest> {

    private static final AtomicLong SESSION_IDS = new AtomicLong();

    private final ServerContext server;
    private ClientSession session;
    private Executor executor;

    public RespServerHandler(ServerContext server) {
        this.server = server;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        session = server.openSession(new ChannelOutput(ctx.channel()),
                Long.toString(SESSION_IDS.incrementAndGet()));
        if (server.config().executionModel() == ServerConfig.ExecutionModel.VIRTUAL_THREADS) {
            executor = new SerialExecutor(server.commandExecutor());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (session != null) {
            server.closeSession(session);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RespRequest request) {
        if (executor == null) {
            RespValue reply = CommandDispatcher.dispatch(server, session, request.args());
            if (reply != null) {
                ctx.write(reply);
            }
            closeIfRequested(ctx);
        } else {
            executor.execute(() -> {
                RespValue reply = CommandDispatcher.dispatch(server, session, request.args());
                if (reply != null) {
                    ctx.writeAndFlush(reply);
                }
                closeIfRequested(ctx);
            });
        }
    }

    private void closeIfRequested(ChannelHandlerContext ctx) {
        if (session.closeAfterReply()) {
            ctx.writeAndFlush(io.netty.buffer.Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Throwable root = cause instanceof DecoderException && cause.getCause() != null
                ? cause.getCause()
                : cause;
        if (root instanceof RespProtocolException) {
            // The framing is broken, so the stream can't be resynchronised: report and close.
            ctx.writeAndFlush(RespValue.error(root.getMessage()))
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }
        if (root instanceof java.io.IOException) {
            // Client vanished mid-write; nothing to report.
            ctx.close();
            return;
        }
        System.err.println("[server] connection error: " + root);
        ctx.close();
    }

    /** Bridges the Pub/Sub push path onto a Netty channel. */
    private record ChannelOutput(Channel channel) implements ClientOutput {

        @Override
        public void push(RespValue value) {
            if (channel.isActive()) {
                channel.writeAndFlush(value);
            }
        }

        @Override
        public boolean isOpen() {
            return channel.isActive();
        }

        @Override
        public String remoteAddress() {
            return String.valueOf(channel.remoteAddress());
        }
    }
}
