package com.rishi.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * TD-2 — the RESP2 request parser.
 *
 * <p>TCP is a byte stream: one command may arrive split across several reads, and
 * one read may carry a hundred pipelined commands. This decoder therefore treats
 * the accumulated {@link ByteBuf} as the parser's state machine — the reader index
 * <em>is</em> the {@code READ_TYPE -> READ_LENGTH -> READ_CONTENT} position. Every
 * frame is attempted from a marked index; the moment the buffer runs dry mid-frame
 * the index is rewound and the decoder returns, waiting for the next read. Nothing
 * is emitted until a semantically complete frame exists, and the loop keeps going
 * so a pipelined batch is decoded in one pass.
 *
 * <p>CRLF placement is validated strictly and every length is bounded, which is what
 * stops a malicious client from smuggling a second command inside a bulk payload or
 * pinning unbounded memory with a huge declared length.
 *
 * <p>Inline commands (a bare {@code PING\r\n} from telnet or netcat) are supported
 * too, exactly as real Redis does.
 */
public final class RespDecoder extends ByteToMessageDecoder {

    /** Longest accepted inline command or length line. */
    static final int MAX_INLINE_LENGTH = 64 * 1024;
    /** Most elements accepted in one multibulk request. */
    static final int MAX_MULTIBULK_LENGTH = 1024 * 1024;
    /** Largest accepted bulk argument: 512 MB, same ceiling as Redis. */
    static final int MAX_BULK_LENGTH = 512 * 1024 * 1024;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.isReadable()) {
            int frameStart = in.readerIndex();
            RespRequest request = parseRequest(in);
            if (request == null) {
                // Incomplete frame: rewind and wait for more bytes.
                in.readerIndex(frameStart);
                break;
            }
            if (!request.isEmpty()) {
                out.add(request);
            }
        }
    }

    /** Parses one request, or returns {@code null} if the buffer holds only part of one. */
    private static RespRequest parseRequest(ByteBuf in) {
        byte first = in.getByte(in.readerIndex());
        return first == '*' ? parseMultiBulk(in) : parseInline(in);
    }

    private static RespRequest parseMultiBulk(ByteBuf in) {
        in.skipBytes(1); // '*'
        String countLine = readCrlfLine(in);
        if (countLine == null) {
            return null;
        }
        int count = parseCount(countLine, "invalid multibulk length");
        if (count > MAX_MULTIBULK_LENGTH) {
            throw new RespProtocolException("ERR Protocol error: invalid multibulk length");
        }
        if (count <= 0) {
            return new RespRequest(List.of());
        }

        List<String> args = new ArrayList<>(Math.min(count, 64));
        for (int i = 0; i < count; i++) {
            if (!in.isReadable()) {
                return null;
            }
            byte type = in.readByte();
            if (type != '$') {
                throw new RespProtocolException(
                        "ERR Protocol error: expected '$', got '" + printable(type) + "'");
            }
            String lengthLine = readCrlfLine(in);
            if (lengthLine == null) {
                return null;
            }
            int length = parseCount(lengthLine, "invalid bulk length");
            if (length < 0 || length > MAX_BULK_LENGTH) {
                throw new RespProtocolException("ERR Protocol error: invalid bulk length");
            }
            if (in.readableBytes() < length + 2) {
                return null;
            }
            args.add(in.toString(in.readerIndex(), length, Bytes.WIRE));
            in.skipBytes(length);
            if (in.readByte() != '\r' || in.readByte() != '\n') {
                throw new RespProtocolException("ERR Protocol error: expected CRLF after bulk payload");
            }
        }
        return new RespRequest(args);
    }

    private static RespRequest parseInline(ByteBuf in) {
        String line = readInlineLine(in);
        if (line == null) {
            return null;
        }
        if (line.isBlank()) {
            return new RespRequest(List.of());
        }
        try {
            return new RespRequest(CommandLineSplitter.split(line));
        } catch (IllegalArgumentException e) {
            throw new RespProtocolException("ERR Protocol error: unbalanced quotes in request");
        }
    }

    /** Reads a CRLF-terminated line, or returns {@code null} when it is not complete yet. */
    private static String readCrlfLine(ByteBuf in) {
        int start = in.readerIndex();
        int end = in.writerIndex();
        for (int i = start; i < end - 1; i++) {
            if (in.getByte(i) == '\r' && in.getByte(i + 1) == '\n') {
                String line = in.toString(start, i - start, Bytes.WIRE);
                in.readerIndex(i + 2);
                return line;
            }
        }
        if (end - start > MAX_INLINE_LENGTH) {
            throw new RespProtocolException("ERR Protocol error: too big inline request");
        }
        return null;
    }

    /** Like {@link #readCrlfLine} but tolerates a bare LF, as telnet clients send. */
    private static String readInlineLine(ByteBuf in) {
        int start = in.readerIndex();
        int end = in.writerIndex();
        for (int i = start; i < end; i++) {
            if (in.getByte(i) == '\n') {
                int lineEnd = (i > start && in.getByte(i - 1) == '\r') ? i - 1 : i;
                String line = in.toString(start, lineEnd - start, Bytes.WIRE);
                in.readerIndex(i + 1);
                return line;
            }
        }
        if (end - start > MAX_INLINE_LENGTH) {
            throw new RespProtocolException("ERR Protocol error: too big inline request");
        }
        return null;
    }

    private static int parseCount(String text, String what) {
        if (text.isEmpty()) {
            throw new RespProtocolException("ERR Protocol error: " + what);
        }
        try {
            long value = Long.parseLong(text);
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                throw new RespProtocolException("ERR Protocol error: " + what);
            }
            return (int) value;
        } catch (NumberFormatException e) {
            throw new RespProtocolException("ERR Protocol error: " + what);
        }
    }

    private static String printable(byte b) {
        char c = (char) (b & 0xFF);
        return c >= 32 && c < 127 ? String.valueOf(c) : "\\x" + String.format("%02x", b & 0xFF);
    }
}
