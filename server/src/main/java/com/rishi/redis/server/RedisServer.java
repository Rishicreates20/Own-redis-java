package com.rishi.redis.server;

import com.rishi.redis.protocol.RespDecoder;
import com.rishi.redis.protocol.RespEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * TD-1 — the RESP listener.
 *
 * <p>Netty's reactor model multiplexes every connection over a small pool of event
 * loops, so ten thousand idle clients cost ten thousand sockets rather than ten
 * thousand threads and their stacks. The boss group only accepts; the worker group
 * owns the sockets, and each socket is pinned to one loop for its lifetime — which is
 * why a connection's handler state needs no synchronisation of its own.
 *
 * <p>On Linux, swapping {@code NioEventLoopGroup}/{@code NioServerSocketChannel} for
 * the Epoll pair (add {@code netty-transport-native-epoll} with the
 * {@code linux-x86_64} classifier and guard on {@code Epoll.isAvailable()}) shaves a
 * layer of JDK indirection off every read; the rest of the pipeline is unchanged.
 */
public final class RedisServer {

    private final ServerContext server;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public RedisServer(ServerContext server) {
        this.server = server;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1, namedThreads("redis-accept"));
        workerGroup = new NioEventLoopGroup(namedThreads("redis-io"));

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 512)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                                .addLast("resp-decoder", new RespDecoder())
                                .addLast("resp-encoder", RespEncoder.INSTANCE)
                                .addLast("commands", new RespServerHandler(server));
                    }
                });

        serverChannel = bootstrap
                .bind(new InetSocketAddress(server.config().host(), server.config().port()))
                .sync()
                .channel();
    }

    /** Blocks until the listening socket closes. */
    public void awaitShutdown() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    static java.util.concurrent.ThreadFactory namedThreads(String prefix) {
        java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
    }
}
