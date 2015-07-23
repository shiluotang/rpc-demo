package org.sqg.thrift;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Every node server implementation basic functionality.
 *
 * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
 * @version 2015/7/7 14:41
 * @since 1.0
 */
public abstract class NodeServer implements AutoCloseable {

    /**
     * SLF4J logger.
     */
    private static final Logger LOGGER = LoggerFactory
            .getLogger(NodeServer.class);

    /**
     * The container server which is used for listen on port and dispatch
     * requests.
     */
    private ThriftServiceContainerServer containerServer;

    /**
     * The port this server be bound.
     */
    private final SocketAddress localAddress;

    /**
     * Construct {@link NodeServer} instance.
     *
     * @param port The port this server will be bound.
     * @since 1.0
     */
    public NodeServer(final int port) {
        localAddress = new InetSocketAddress(port);
        try {
            containerServer = new ThriftServiceContainerServer(port, null);
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    /**
     * Start this server in block manner.
     *
     * @since 1.0
     */
    public final void start() {
        synchronized (this) {
            if (containerServer != null) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Startup node server start...");
                try {
                    containerServer.start();
                    login();
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Startup node server stopped.");
                } catch (Throwable t) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Startup node server failed!!");
                    throw t;
                }
            }
        }
    }

    /**
     * Stop this server in block manner.
     *
     * @since 1.0
     */
    public final void stop() {
        synchronized (this) {
            if (containerServer != null) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Shutdown node server start...");
                try {
                    logout();
                    containerServer.close();
                    containerServer = null;
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Shutdown node server stopped.");
                } catch (Throwable t) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Shutdown node server failed!!");
                    throw t;
                }
            }
        }
    }

    /**
     * Get the port this server is bound.
     *
     * @return The address this server is bound to.
     * @since 1.0
     */
    public final SocketAddress getLocalAddress() {
        return localAddress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void close() {
        stop();
    }

    /**
     * Login to the register center.
     */
    protected final void login() {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Login node server into central.");
        // TODO Not implemented!
    }

    /**
     * Logout from the register center.
     */
    protected final void logout() {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Logout node server from central.");
        // TODO Not implemented!
    }

    /**
     * Handle all the online node server list retrieved on login.
     */
    protected final void handleOnlineNodes() {
        // TODO Not implemented!
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final String toString() {
        return String.valueOf(getLocalAddress());
    }
}
