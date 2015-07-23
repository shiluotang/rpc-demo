package org.sqg.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;

import org.sqg.util.JdkSerializer;
import org.sqg.util.Serializer;

public final class RpcService implements AutoCloseable {

    public static final Serializer SERIALIZER = new JdkSerializer();

    private int port;

    public RpcService(final int port) {
        this.port = port;
    }

    public void start() {
        NioEventLoopGroup globalGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group();
    }

    @Override
    public void close() {
    }
}
