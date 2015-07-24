package org.sqg.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RpcClient.class);

    private final class RpcResponseHandler extends
            SimpleChannelInboundHandler<RpcResponse> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg)
                throws Exception {
            responses.put(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            LOGGER.info("caught exception!!!");
            RpcResponse response = new RpcResponse();
            response.setThrowable(cause);
            responses.put(response);
        }
    }

    private InetSocketAddress remoteServerAddress;

    private Bootstrap bootstrap;
    private EventLoopGroup workerGroup;
    private Channel channel;

    private BlockingQueue<RpcResponse> responses;

    public RpcClient(final SocketAddress remoteServerAddress) {
        this.remoteServerAddress = (InetSocketAddress) remoteServerAddress;
        this.responses = new ArrayBlockingQueue<>(10);
        initBootstrap();
    }

    private void initBootstrap() {
        try {
            bootstrap = new Bootstrap();
            workerGroup = new NioEventLoopGroup();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    if (ch.pipeline().get("requestEncoder") == null)
                        ch.pipeline().addLast("requestEncoder",
                                new RpcRequest.Encoder());
                    if (ch.pipeline().get("responseDecoder") == null)
                        ch.pipeline().addLast("responseDecoder",
                                new RpcResponse.Decoder());
                    if (ch.pipeline().get("responseReceiver") == null)
                        ch.pipeline().addLast("responseReceiver",
                                new RpcResponseHandler());
                }
            });
            bootstrap.option(ChannelOption.TCP_NODELAY, Boolean.TRUE);
            channel = bootstrap.connect(remoteServerAddress).sync().channel();
            LOGGER.info("connected to {}.", remoteServerAddress);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public RpcResponse doRpcCall(final RpcRequest request) {
        try {
            if (channel == null)
                channel = bootstrap
                        .connect(remoteServerAddress.getHostString(),
                                remoteServerAddress.getPort()).sync().channel();
            channel.writeAndFlush(request).sync();
            RpcResponse response = responses.take();
            LOGGER.debug("RESPONSE is {}", response);
            return response;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public SocketAddress getRemoteServerAddress() {
        return remoteServerAddress;
    }

    @Override
    public void close() {
        if (channel != null) {
            try {
                channel.close().sync();
                channel = null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (workerGroup != null)
            workerGroup.shutdownGracefully(10L, 1000L, TimeUnit.MILLISECONDS);
        if (bootstrap != null)
            bootstrap = null;
    }
}
