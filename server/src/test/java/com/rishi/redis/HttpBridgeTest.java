package com.rishi.redis;

import com.rishi.redis.server.HttpCommandHandler;
import com.rishi.redis.server.Json;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The contract the React Web CLI is written against: POST a command, get
 * {@code {"result": ...}} back, with CORS headers that let a browser on another
 * origin read the response.
 */
class HttpBridgeTest {

    private ServerHarness redis;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        redis = new ServerHarness();
        channel = new EmbeddedChannel(new HttpCommandHandler(redis.context()));
    }

    private Map<String, String> post(String json) {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/command", Unpooled.copiedBuffer(json, StandardCharsets.UTF_8));
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        channel.writeInbound(request);
        FullHttpResponse response = channel.readOutbound();
        assertNotNull(response, "handler produced no response");
        assertEquals(HttpResponseStatus.OK, response.status());
        assertEquals("*", response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN));
        String body = response.content().toString(StandardCharsets.UTF_8);
        response.release();
        return Json.parseObject(body);
    }

    @Test
    void runsACommandAndReturnsTheResultField() {
        assertEquals("OK", post(Json.object("command", "SET mykey Hello")).get("result"));
        assertEquals("Hello", post(Json.object("command", "GET mykey")).get("result"));
        assertEquals("(integer) 1", post(Json.object("command", "DEL mykey")).get("result"));
    }

    @Test
    void quotedArgumentsSurviveTheJsonRoundTrip() {
        post(Json.object("command", "SET greeting \"hello world\""));
        assertEquals("hello world", post(Json.object("command", "GET greeting")).get("result"));
    }

    @Test
    void redisErrorsComeBackAsAResultNotAnHttpFailure() {
        Map<String, String> reply = post(Json.object("command", "INCR"));
        assertEquals("(error) ERR wrong number of arguments for 'incr' command", reply.get("result"));
        assertEquals("true", reply.get("error"));
    }

    @Test
    void sessionStateIsKeptPerSessionId() {
        post(Json.object("command", "SELECT 2", "sessionId", "tab-a"));
        post(Json.object("command", "SET scoped yes", "sessionId", "tab-a"));

        // A different tab is still on db 0 and must not see the key.
        assertEquals("(nil)", post(Json.object("command", "GET scoped", "sessionId", "tab-b")).get("result"));
        assertEquals("yes", post(Json.object("command", "GET scoped", "sessionId", "tab-a")).get("result"));
    }

    @Test
    void multiExecWorksAcrossSeparateHttpRequests() {
        post(Json.object("command", "MULTI", "sessionId", "tab-a"));
        assertEquals("QUEUED", post(Json.object("command", "SET a 1", "sessionId", "tab-a")).get("result"));
        assertEquals("1) OK", post(Json.object("command", "EXEC", "sessionId", "tab-a")).get("result"));
        assertEquals("1", post(Json.object("command", "GET a", "sessionId", "tab-a")).get("result"));
    }

    @Test
    void publishedMessagesRideBackOnTheNextResponse() {
        post(Json.object("command", "SUBSCRIBE news", "sessionId", "tab-a"));
        post(Json.object("command", "PUBLISH news hello", "sessionId", "tab-b"));

        String result = post(Json.object("command", "PING", "sessionId", "tab-a")).get("result");
        assertTrue(result.contains("hello"), "expected the pushed message in: " + result);
    }

    @Test
    void healthEndpointReportsTheServer() {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/health", Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request);
        FullHttpResponse response = channel.readOutbound();
        Map<String, String> body = Json.parseObject(response.content().toString(StandardCharsets.UTF_8));
        response.release();
        assertEquals("ok", body.get("status"));
        assertEquals("own-redis-java", body.get("server"));
    }

    @Test
    void preflightIsAnswered() {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS,
                "/api/command", Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request);
        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.NO_CONTENT, response.status());
        assertTrue(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS).contains("POST"));
        response.release();
    }
}
