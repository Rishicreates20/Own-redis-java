package com.rishi.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

/**
 * Serialises a {@link RespValue} tree onto the wire. Stateless, so a single
 * instance is shared by every channel.
 */
@ChannelHandler.Sharable
public final class RespEncoder extends MessageToByteEncoder<RespValue> {

    public static final RespEncoder INSTANCE = new RespEncoder();

    private static final byte[] NULL_BULK = {'$', '-', '1', '\r', '\n'};
    private static final byte[] NULL_ARRAY = {'*', '-', '1', '\r', '\n'};

    @Override
    protected void encode(ChannelHandlerContext ctx, RespValue msg, ByteBuf out) {
        write(msg, out);
    }

    /** Appends the RESP2 encoding of {@code value} to {@code out}. */
    public static void write(RespValue value, ByteBuf out) {
        switch (value) {
            case RespValue.SimpleString s -> {
                out.writeByte('+');
                writeLine(out, s.value());
            }
            case RespValue.Error e -> {
                out.writeByte('-');
                writeLine(out, e.message());
            }
            case RespValue.Integer64 i -> {
                out.writeByte(':');
                writeLine(out, Long.toString(i.value()));
            }
            case RespValue.BulkString b -> {
                String payload = b.value();
                if (payload == null) {
                    out.writeBytes(NULL_BULK);
                } else {
                    out.writeByte('$');
                    // Wire strings are ISO-8859-1, so char count == byte count.
                    writeLine(out, Integer.toString(payload.length()));
                    writeLine(out, payload);
                }
            }
            case RespValue.Array a -> {
                List<RespValue> items = a.items();
                if (items == null) {
                    out.writeBytes(NULL_ARRAY);
                } else {
                    out.writeByte('*');
                    writeLine(out, Integer.toString(items.size()));
                    for (RespValue item : items) {
                        write(item, out);
                    }
                }
            }
        }
    }

    private static void writeLine(ByteBuf out, String text) {
        out.writeCharSequence(text, Bytes.WIRE);
        out.writeByte('\r');
        out.writeByte('\n');
    }
}
