package org.sqg.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.rpc.RpcRequest;
import org.sqg.rpc.RpcResponse;

public final class RpcServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RpcServer.class);

    public final class RpcRequestHandler extends IoHandlerAdapter {

        private org.sqg.rpc.RpcRequestHandler handler;

        public RpcRequestHandler() {
            this(null);
        }

        public RpcRequestHandler(Object[] serviceImplementors) {
            handler = new org.sqg.rpc.RpcRequestHandler(serviceImplementors);
        }

        @Override
        public void messageReceived(IoSession session, Object message)
                throws Exception {
            if (message instanceof RpcRequest) {
                final RpcRequest request = (RpcRequest) message;
                Object servant = handler.getServant(request);
                if (servant == null)
                    return;
                session.write(handler.processRequest(request, servant));
            }
        }
    }

    private int port;
    private volatile IoAcceptor service;
    private RpcRequestHandler handler;

    public RpcServer(final int port, Object... serviceImplementors) {
        this.port = port;
        this.handler = new RpcRequestHandler(serviceImplementors);
    }

    public SocketAddress getLocalAddress() {
        return new InetSocketAddress(port);
    }

    public void start() {
        if (service == null) {
            synchronized (this) {
                if (service == null) {
                    LOGGER.debug("start server at {} start...",
                            getLocalAddress());
                    try {
                        NioSocketAcceptor service = new NioSocketAcceptor();
                        service.getFilterChain().addLast(
                                "demux-codec",
                                new ProtocolCodecFilter(new Codec<>(
                                        RpcRequest.class, RpcResponse.class)));
                        service.setHandler(handler);
                        service.bind(new InetSocketAddress(port));
                        this.service = service;
                    } catch (IOException e) {
                        if (service != null) {
                            service.dispose();
                            service = null;
                        }
                        throw new RuntimeException(e);
                    }
                    LOGGER.debug("start server at {} stopped.",
                            getLocalAddress());
                }
            }
        }
    }

    public void stop() {
        if (service != null) {
            synchronized (this) {
                if (service != null) {
                    LOGGER.debug("stop server at {} start...", getLocalAddress());
                    service.unbind();
                    service.dispose();
                    service = null;
                    LOGGER.debug("stop server at {} stopped.", getLocalAddress());
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
        handler = null;
    }
}
