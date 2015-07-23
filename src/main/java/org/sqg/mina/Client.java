package org.sqg.mina;

import java.net.SocketAddress;

import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoService;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

public abstract class Client extends MultipartIoService {

    private IoSession session;

    public Client(SocketAddress remoteServerAddress) {
        super.initService();
        try {
            session = ((IoConnector) service).connect(remoteServerAddress)
                    .await().getSession();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public SocketAddress getLocalAddress() {
        if (session != null)
            return session.getLocalAddress();
        return null;
    }

    @Override
    public void close() {
        if (session != null) {
            session.close(true);
            session = null;
        }
        super.close();
    }

    public <T> void send(T data) {
        sendMessage(session, data);
    }

    @Override
    protected IoService createIoService() {
        return new NioSocketConnector();
    }

}
