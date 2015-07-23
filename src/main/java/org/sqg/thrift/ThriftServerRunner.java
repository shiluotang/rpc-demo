package org.sqg.thrift;

import java.lang.ref.WeakReference;

import org.apache.thrift.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable for start thrift server.
 *
 * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
 * @version 2015/7/6 19:43
 * @since 1.0
 */
public final class ThriftServerRunner implements Runnable {

    /**
     * SLF4J logger.
     */
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ThriftServerRunner.class);

    /**
     * Hold a weak reference to thrift server instance.
     */
    private WeakReference<TServer> thriftServerRef;

    /**
     * Construct a {@link ThriftServerRunner} instance.
     *
     * @param thriftServer The already instantiated thrift server instance.
     */
    public ThriftServerRunner(final TServer thriftServer) {
        thriftServerRef = new WeakReference<TServer>(thriftServer);
    }

    @Override
    public void run() {
        TServer server = thriftServerRef.get();
        if (server != null) {
            LOGGER.info("thrift server {} start...", server);
            server.serve();
            LOGGER.info("thrift server {} stopped.", server);
        }
    }
}
