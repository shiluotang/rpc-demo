package org.sqg.thrift;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sqg.thrift.NamedThreadFactory;

/**
 * The container server which will hold serveral thrift iface to provide RPC
 * service.
 *
 * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
 * @version 2015/7/3 14:21
 * @since 1.0
 */
public final class ThriftServiceContainerServer implements AutoCloseable {

    /**
     * SLF4J logger.
     */
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ThriftServiceContainerServer.class);

    /**
     * This is used to check whether server is started or stopped.
     */
    private static final long CHECK_INTERVAL_MILLIS = 10L;

    /**
     * Used for start thrift service asynchrously.
     */
    private final ExecutorService threadPool;

    /**
     * Store the server parameters.
     */
    private final ThriftServerBuilder builder;

    /**
     * The thrift server instance.
     */
    private TServer thriftServer;

    /**
     * Construct {@link ThriftServiceContainerServer} instance.
     *
     * @param timeoutUnit
     *            This RPC service server-side timeout duration measurement.
     * @param timeout
     *            This RPC service server-side timeout duration.
     * @param port
     *            On which port this RPC service will be running.
     * @param ifaceImplementors
     *            The objects which has implemented some thrift Iface interface.
     *            Usually they should stick to different interfaces to prevent
     *            chaos.
     */
    public ThriftServiceContainerServer(final TimeUnit timeoutUnit,
            final long timeout, final int port, final Object[] ifaceImplementors) {
        threadPool = Executors.newFixedThreadPool(1, new NamedThreadFactory(
                "thriftserivce-container"));
        builder = ThriftServerBuilder.nonblocking();
        builder.listen(port);
        if (timeout >= 0)
            builder.timeout(timeout, timeoutUnit);
        if (ifaceImplementors != null)
            for (Object ifaceImplementor : ifaceImplementors)
                if (ifaceImplementor != null)
                    builder.addAllImplementedProcessors(ifaceImplementor);
    }

    /**
     * Construct {@link ThriftServiceContainerServer} instance.
     *
     * @param port
     *            On which port this RPC service will be running.
     * @param ifaceImplementors
     *            The objects which has implemented some thrift Iface interface.
     *            Usually they should stick to different interfaces to prevent
     *            chaos.
     */
    public ThriftServiceContainerServer(final int port,
            final Object[] ifaceImplementors) {
        this(TimeUnit.SECONDS, -1, port, ifaceImplementors);
    }

    /**
     * Start thrift server.
     * <p>
     * This service will start in blocking way.
     * </p>
     *
     * @since 1.0
     */
    public void start() {
        synchronized (this) {
            if (thriftServer == null) {
                try {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Start server start...");
                    thriftServer = builder.build();
                    threadPool.submit(new ThriftServerRunner(thriftServer));
                    while (!thriftServer.isServing()) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL_MILLIS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Start server stopped.");
                } catch (Throwable t) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Start server failed!!");
                    throw t;
                }
            }
        }
    }

    /**
     * Stop thrift server.
     * <p>
     * This service will start in blocking way.
     * </p>
     *
     * @since 1.0
     */
    public void stop() {
        synchronized (this) {
            if (thriftServer != null) {
                try {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Stop server start...");
                    thriftServer.stop();
                    while (thriftServer.isServing()) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL_MILLIS);
                        } catch (InterruptedException e) {
                            LOGGER.warn("exit before shutdown complete.");
                            break;
                        }
                    }
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Stop server stopped.");
                    thriftServer = null;
                } catch (Throwable t) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Stop server failed!!");
                    throw t;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        stop();
        if (threadPool != null) {
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(CHECK_INTERVAL_MILLIS,
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            threadPool.shutdownNow();
        }
    }
}
