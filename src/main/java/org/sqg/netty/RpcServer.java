package org.sqg.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.util.JdkSerializer;
import org.sqg.util.Serializer;

public final class RpcServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RpcServer.class);

    public static final Serializer SERIALIZER = new JdkSerializer();

    private int port;
    private volatile Channel ch;
    private NioEventLoopGroup acceptorGroup;
    private NioEventLoopGroup workerGroup;
    private ServerBootstrap bootstrap;
    private RpcRequestHandler handler;

    public RpcServer(final int port) {
        this(port, null);
    }

    public RpcServer(final int port, final Object[] serviceImplementors) {
        this.port = port;
        this.handler = new RpcRequestHandler(serviceImplementors);
    }

    public SocketAddress getLocalAddress() {
        if (ch == null)
            return new InetSocketAddress(port);
        return ch.localAddress();
    }

    public void start() throws InterruptedException {
        if (ch == null) {
            synchronized (this) {
                if (ch == null) {
                    LOGGER.info("starting server at {} ...", getLocalAddress());
                    acceptorGroup = new NioEventLoopGroup();
                    workerGroup = new NioEventLoopGroup();
                    bootstrap = new ServerBootstrap();
                    bootstrap.group(acceptorGroup, workerGroup);
                    bootstrap.channel(NioServerSocketChannel.class);
                    bootstrap
                            .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch)
                                        throws Exception {
                                    ch.pipeline()
                                            .addLast("requestDecoder",
                                                    new RpcRequest.Decoder())
                                            .addLast("responseEncoder",
                                                    new RpcResponse.Encoder())
                                            .addLast("requestHandler", handler);
                                }
                            });
                    ChannelFuture future = bootstrap.bind(port);
                    future.sync();
                    ch = future.channel();
                    LOGGER.info("started server at {}.", getLocalAddress());
                }
            }
        }
    }

    public void stop() {
        if (ch != null) {
            synchronized (this) {
                if (ch != null) {
                    LOGGER.info("stopping server at {} ...", getLocalAddress());
                    if (ch != null) {
                        try {
                            LOGGER.info("shutdown bootstrap channel start...");
                            ch.close().await();
                            ch = null;
                            LOGGER.info("shutdown bootstrap channel stopped.");
                            LOGGER.info("shutdown acceptor group start...");
                            acceptorGroup.shutdownGracefully().await();
                            LOGGER.info("shutdown acceptor group stopped.");
                            LOGGER.info("shutdown worker group start...");
                            workerGroup.shutdownGracefully().await();
                            LOGGER.info("shutdown worker group stopped.");
                            LOGGER.info("stopped server at {}.",
                                    getLocalAddress());
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
    }
}
