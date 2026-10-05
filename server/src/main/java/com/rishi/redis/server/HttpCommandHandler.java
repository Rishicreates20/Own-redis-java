package com.rishi.redis.server;

import com.rishi.redis.command.CommandDispatcher;
import com.rishi.redis.protocol.Bytes;
import com.rishi.redis.protocol.CommandLineSplitter;
import com.rishi.redis.protocol.RespValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The REST bridge the React Web CLI talks to.
 *
 * <p>Contract, matching the frontend exactly:
 * <pre>
 *   POST /api/command   {"command": "SET mykey Hello"}   ->  {"result": "OK"}
 *   GET  /api/health                                     ->  {"status": "ok", ...}
 *   GET  /api/messages?sessionId=...                     ->  queued Pub/Sub pushes
 * </pre>
 *
 * <p>The command text is split with the same argument splitter the inline RESP path
 * uses, then handed to the very same dispatcher a redis-cli connection would reach —
 * the bridge is a second front door onto one implementation, not a reimplementation.
 * Replies are rendered in redis-cli's own notation so the terminal pane reads the way
 * a real session does.
 *
 * <p>A command that fails still comes back as HTTP 200 with the error in
 * {@code result}: an error reply is a normal outcome of a Redis command, and turning
 * it into an HTTP failure would make the UI report a connection problem instead.
 */
public final class HttpCommandHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final ServerContext server;

    public HttpCommandHandler(ServerContext server) {
        this.server = server;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String path = new QueryStringDecoder(request.uri()).path();

        if (request.method() == HttpMethod.OPTIONS) {
            respond(ctx, request, HttpResponseStatus.NO_CONTENT, "");
            return;
        }
        try {
            if (path.equals("/api/command") && request.method() == HttpMethod.POST) {
                handleCommand(ctx, request);
            } else if (path.equals("/api/health") && request.method() == HttpMethod.GET) {
                handleHealth(ctx, request);
            } else if (path.equals("/api/messages") && request.method() == HttpMethod.GET) {
                handleMessages(ctx, request);
            } else {
                respond(ctx, request, HttpResponseStatus.NOT_FOUND,
                        Json.object("error", "no route for " + request.method() + " " + path,
                                "routes", "POST /api/command, GET /api/health, GET /api/messages"));
            }
        } catch (IllegalArgumentException e) {
            respond(ctx, request, HttpResponseStatus.BAD_REQUEST,
                    Json.object("result", "(error) ERR " + e.getMessage(), "error", true));
        } catch (RuntimeException e) {
            System.err.println("[http] request failed: " + e);
            respond(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    Json.object("result", "(error) ERR internal server error", "error", true));
        }
    }

    private void handleCommand(ChannelHandlerContext ctx, FullHttpRequest request) {
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, String> fields = body.isBlank() ? Map.of() : Json.parseObject(body);

        String commandText = fields.get("command");
        if (commandText == null || commandText.isBlank()) {
            respond(ctx, request, HttpResponseStatus.OK,
                    Json.object("result", "(error) ERR empty command", "error", true));
            return;
        }

        ClientSession session = resolveSession(ctx, request, fields);
        List<String> args;
        try {
            args = CommandLineSplitter.split(Bytes.fromText(commandText));
        } catch (IllegalArgumentException e) {
            respond(ctx, request, HttpResponseStatus.OK,
                    Json.object("result", "(error) ERR Protocol error: " + e.getMessage(),
                            "error", true, "command", commandText));
            return;
        }

        long startNanos = System.nanoTime();
        RespValue reply = CommandDispatcher.dispatch(server, session, args);
        long micros = (System.nanoTime() - startNanos) / 1000L;

        StringBuilder text = new StringBuilder();
        if (reply != null) {
            text.append(ReplyFormatter.toText(reply));
        }
        // Fold in anything Pub/Sub pushed to this session (SUBSCRIBE confirmations and
        // messages published since the last request).
        for (RespValue pushed : drain(session)) {
            if (text.length() > 0) {
                text.append('\n');
            }
            text.append(ReplyFormatter.toText(pushed));
        }
        if (text.length() == 0) {
            text.append("OK");
        }

        respond(ctx, request, HttpResponseStatus.OK, Json.object(
                "result", text.toString(),
                "error", reply != null && ReplyFormatter.isError(reply),
                "command", commandText,
                "sessionId", session.id(),
                "db", session.databaseIndex(),
                "durationMicros", micros));
    }

    private void handleHealth(ChannelHandlerContext ctx, FullHttpRequest request) {
        respond(ctx, request, HttpResponseStatus.OK, Json.object(
                "status", "ok",
                "server", "own-redis-java",
                "respPort", server.config().port(),
                "httpPort", server.config().httpPort(),
                "uptimeSeconds", server.uptimeSeconds(),
                "connectedClients", server.connectedClients(),
                "commandsProcessed", server.commandsProcessed(),
                "databases", server.databaseCount()));
    }

    private void handleMessages(ChannelHandlerContext ctx, FullHttpRequest request) {
        QueryStringDecoder query = new QueryStringDecoder(request.uri());
        List<String> ids = query.parameters().getOrDefault("sessionId", List.of());
        String id = ids.isEmpty() ? "web:" + clientHost(ctx) : ids.get(0);
        ClientSession session = server.httpSession(id, new HttpSessionOutput(remoteAddress(ctx)));

        StringBuilder text = new StringBuilder();
        for (RespValue pushed : drain(session)) {
            if (text.length() > 0) {
                text.append('\n');
            }
            text.append(ReplyFormatter.toText(pushed));
        }
        respond(ctx, request, HttpResponseStatus.OK,
                Json.object("result", text.toString(), "sessionId", id, "error", false));
    }

    private ClientSession resolveSession(ChannelHandlerContext ctx, FullHttpRequest request,
                                         Map<String, String> fields) {
        String id = fields.get("sessionId");
        if (id == null || id.isBlank()) {
            id = request.headers().get("X-Session-Id");
        }
        if (id == null || id.isBlank()) {
            id = "web:" + clientHost(ctx);
        }
        return server.httpSession(id, new HttpSessionOutput(remoteAddress(ctx)));
    }

    /**
     * The fallback session key when the caller supplies none. It deliberately drops the
     * ephemeral port: a browser opens several parallel connections to one origin, and
     * keying on host:port would scatter one tab's SELECT and MULTI state across several
     * sessions. Callers that want isolation should send their own {@code sessionId}.
     */
    private static String clientHost(ChannelHandlerContext ctx) {
        if (ctx.channel().remoteAddress() instanceof java.net.InetSocketAddress address) {
            return address.getAddress() == null
                    ? address.getHostString()
                    : address.getAddress().getHostAddress();
        }
        return remoteAddress(ctx);
    }

    private List<RespValue> drain(ClientSession session) {
        if (session.output() instanceof HttpSessionOutput sink) {
            return sink.drain();
        }
        return List.of();
    }

    private static String remoteAddress(ChannelHandlerContext ctx) {
        return String.valueOf(ctx.channel().remoteAddress());
    }

    private void respond(ChannelHandlerContext ctx, FullHttpRequest request,
                         HttpResponseStatus status, String json) {
        ByteBuf content = Unpooled.copiedBuffer(json, StandardCharsets.UTF_8);
        FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-store");
        // FR: the Web CLI is served from a different origin (Vite on :3000), so the
        // bridge has to answer preflights and allow the origin explicitly.
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, server.config().corsOrigin());
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS");
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, X-Session-Id");
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, "86400");
        response.headers().set(HttpHeaderNames.VARY, HttpHeaderNames.ORIGIN);

        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        } else {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        }
        ChannelFuture future = ctx.writeAndFlush(response);
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!(cause instanceof java.io.IOException)) {
            System.err.println("[http] connection error: " + cause);
        }
        ctx.close();
    }
}
