package com.rishi.redis.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.net.InetSocketAddress;

/**
 * The HTTP front door for the React Web CLI, on its own port and its own event-loop
 * group so a slow browser request can never sit in front of RESP traffic.
 *
 * <p>Bodies are aggregated (with a 1 MB ceiling) before the handler sees them, so
 * {@link HttpCommandHandler} deals in whole requests rather than in chunks.
 */
public final class HttpBridgeServer {

    private static final int MAX_BODY_BYTES = 1024 * 1024;

    private final ServerContext server;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public HttpBridgeServer(ServerContext server) {
        this.server = server;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1, RedisServer.namedThreads("http-accept"));
        workerGroup = new NioEventLoopGroup(2, RedisServer.namedThreads("http-io"));

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new HttpObjectAggregator(MAX_BODY_BYTES))
                                .addLast(new ChunkedWriteHandler())
                                .addLast(new HttpCommandHandler(server));
                    }
                });

        serverChannel = bootstrap
                .bind(new InetSocketAddress(server.config().host(), server.config().httpPort()))
                .sync()
                .channel();
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
}
