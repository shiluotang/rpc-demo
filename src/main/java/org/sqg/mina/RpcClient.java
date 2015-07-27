package org.sqg.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.sqg.rpc.RpcRequest;
import org.sqg.rpc.RpcResponse;

public final class RpcClient extends org.sqg.rpc.RpcClient {

    private final class RpcResponseHandler extends IoHandlerAdapter {

        @Override
        public void messageReceived(IoSession session, Object message)
                throws Exception {
            if (message instanceof RpcResponse)
                responses.put((RpcResponse) message);
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause)
                throws Exception {
            if (cause instanceof IOException) {
                super.exceptionCaught(session, cause);
                RpcClient.this.session = null;
            }
            responses.put(new RpcResponse(null, cause));
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {
            // TODO Auto-generated method stub
            super.sessionClosed(session);
            RpcClient.this.session = null;
        }
    }

    private IoConnector connector;
    private IoSession session;
    private InetSocketAddress remoteServerAddress;
    private BlockingQueue<RpcResponse> responses;

    public RpcClient(final SocketAddress remoteServerAddress) {
        this.remoteServerAddress = (InetSocketAddress) remoteServerAddress;
        this.responses = new ArrayBlockingQueue<RpcResponse>(10);
        try {
            this.connector = new NioSocketConnector();
            this.connector.getFilterChain().addLast(
                    "demux-codec",
                    new ProtocolCodecFilter(new Codec<>(RpcResponse.class,
                            RpcRequest.class)));
            this.connector.setHandler(new RpcResponseHandler());
            this.session = this.connector.connect(remoteServerAddress).await()
                    .getSession();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    protected void stop() {
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    protected RpcResponse doRPC(final RpcRequest request) {
        try {
            if (session == null)
                session = connector.connect(remoteServerAddress).await()
                        .getSession();
            session.write(request).await();
            return responses.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
