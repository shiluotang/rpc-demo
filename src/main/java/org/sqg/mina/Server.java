package org.sqg.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoService;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public abstract class Server extends MultipartIoService {

    private static final Logger LOGGER = Logger.getLogger(Server.class.getName());

    /**
     * Construct server and start listen.
     *
     * @param port
     *            listen port.
     */
    public Server(int port) {
        initService();
        try {
            SocketAddress address = new InetSocketAddress(port);
            LOGGER.info("server starting at " + address + " ...");
            ((IoAcceptor) service).bind(address);
            LOGGER.info("server started at " + address + " .");
        } catch (UnknownHostException e) {
            // This should not happen.
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SocketAddress getLocalAddress() {
        if (service != null)
            return ((IoAcceptor) service).getLocalAddress();
        return null;
    }

    @Override
    public void close() {
        SocketAddress address = getLocalAddress();
        LOGGER.info("stopping server at " + address + " ...");
        ((IoAcceptor) service).unbind();
        super.close();
        LOGGER.info("stopped server at " + address + " .");
    }

    @Override
    protected IoService createIoService() {
        return new NioSocketAcceptor();
    }
}