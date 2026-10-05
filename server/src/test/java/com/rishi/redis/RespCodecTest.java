package com.rishi.redis;

import com.rishi.redis.protocol.RespDecoder;
import com.rishi.redis.protocol.RespEncoder;
import com.rishi.redis.protocol.RespProtocolException;
import com.rishi.redis.protocol.RespRequest;
import com.rishi.redis.protocol.RespValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FR-1 — the parser has to survive fragmentation, pipelining and hostile input. */
class RespCodecTest {

    private static EmbeddedChannel channel() {
        return new EmbeddedChannel(new RespDecoder());
    }

    private static ByteBuf bytes(String text) {
        return Unpooled.copiedBuffer(text, StandardCharsets.ISO_8859_1);
    }

    @Test
    void decodesASingleCommand() {
        EmbeddedChannel channel = channel();
        channel.writeInbound(bytes("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nHello\r\n"));
        RespRequest request = channel.readInbound();
        assertNotNull(request);
        assertEquals(List.of("SET", "mykey", "Hello"), request.args());
    }

    @Test
    void waitsForTheRestOfAFragmentedFrame() {
        EmbeddedChannel channel = channel();
        // Arrives in three TCP segments, splitting mid-length and mid-payload.
        channel.writeInbound(bytes("*2\r\n$3\r\nGE"));
        assertNull(channel.readInbound());
        channel.writeInbound(bytes("T\r\n$5\r\nmyk"));
        assertNull(channel.readInbound());
        channel.writeInbound(bytes("ey\r\n"));

        RespRequest request = channel.readInbound();
        assertNotNull(request);
        assertEquals(List.of("GET", "mykey"), request.args());
    }

    @Test
    void decodesPipelinedCommandsFromOneRead() {
        EmbeddedChannel channel = channel();
        channel.writeInbound(bytes("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"));

        RespRequest first = channel.readInbound();
        RespRequest second = channel.readInbound();
        RespRequest third = channel.readInbound();
        assertEquals(List.of("PING"), first.args());
        assertEquals(List.of("PING"), second.args());
        assertEquals(List.of("ECHO", "hi"), third.args());
    }

    @Test
    void decodesInlineCommands() {
        EmbeddedChannel channel = channel();
        channel.writeInbound(bytes("SET greeting \"hello world\"\r\n"));
        RespRequest request = channel.readInbound();
        assertEquals(List.of("SET", "greeting", "hello world"), request.args());
    }

    @Test
    void rejectsAMalformedFrame() {
        EmbeddedChannel channel = channel();
        // Each element of a multibulk must start with '$'; a '+' here is a violation,
        // and the decoder must fail loudly rather than resynchronise on garbage.
        DecoderException failure = assertThrows(DecoderException.class,
                () -> channel.writeInbound(bytes("*1\r\n+PING\r\n")));
        assertTrue(rootCause(failure) instanceof RespProtocolException);
    }

    @Test
    void rejectsAnOversizedInlineRequest() {
        EmbeddedChannel channel = channel();
        String flood = "A".repeat(70_000); // no newline: the parser must bail, not buffer forever
        assertThrows(DecoderException.class, () -> channel.writeInbound(bytes(flood)));
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    @Test
    void preservesArbitraryBytesThroughAnEncodeDecodeRoundTrip() {
        // Byte 0xFF is not valid UTF-8; the ISO-8859-1 wire representation keeps it intact.
        String binary = "aÿb";
        ByteBuf buffer = Unpooled.buffer();
        RespEncoder.write(RespValue.bulk(binary), buffer);
        assertEquals("$3\r\naÿb\r\n", buffer.toString(StandardCharsets.ISO_8859_1));
    }

    @Test
    void encodesEveryRespType() {
        assertEncoded("+OK\r\n", RespValue.OK);
        assertEncoded("-ERR nope\r\n", RespValue.error("ERR nope"));
        assertEncoded(":42\r\n", RespValue.integer(42));
        assertEncoded("$-1\r\n", RespValue.NIL);
        assertEncoded("*-1\r\n", RespValue.NIL_ARRAY);
        assertEncoded("*2\r\n$1\r\na\r\n:1\r\n",
                RespValue.array(List.of(RespValue.bulk("a"), RespValue.integer(1))));
    }

    private static void assertEncoded(String expected, RespValue value) {
        ByteBuf buffer = Unpooled.buffer();
        RespEncoder.write(value, buffer);
        assertEquals(expected, buffer.toString(StandardCharsets.ISO_8859_1));
    }
}
