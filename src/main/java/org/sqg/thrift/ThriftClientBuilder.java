package org.sqg.thrift;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Utility for construct a thrift client.
 * <p>
 * Provide settings remote server and timeout.
 * </p>
 *
 * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
 * @version 2015/5/20 18:04:46
 * @since 1.0
 */
public abstract class ThriftClientBuilder {

    /**
     * The TTransport object, which may be constructed outside of this builder.
     */
    private TTransport transport;

    /**
     * Remote server address.
     */
    private InetSocketAddress serverAddress;

    /**
     * Time out duration, which is socket option timeout.
     */
    private long timeout;

    /**
     * The field {@code timeout} measurement.
     */
    private TimeUnit timeoutUnit;

    /**
     * Get the remote server address.
     *
     * @return The remote server address.
     */
    protected final InetSocketAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * Get client socket timeout duration.
     *
     * @return Client socket timeout duration.
     */
    protected final long getTimeoutDuration() {
        return timeout;
    }

    /**
     * Get client socket timeout unit measurement.
     *
     * @return Client socket timeout unit measurement.
     */
    protected final TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    /**
     * Set remote server address.
     *
     * @param hostAndPort {@link InetSocketAddress} provide the host and port
     * parameter.
     * @return {@link ThriftClientBuilder} instance for chained call.
     *
     * @since 1.0
     */
    public final ThriftClientBuilder remoteServer(final InetSocketAddress
            hostAndPort) {
        serverAddress = Objects.requireNonNull(hostAndPort);
        return this;
    }

    /**
     * Set remote server address.
     *
     * @param host Remote server host, can either be ip string or host name.
     * @param port Remote server port number.
     * @return {@link ThriftClientBuilder} instance for chained call.
     *
     * @since 1.0
     */
    public final ThriftClientBuilder remoteServer(final String host, final int port) {
        serverAddress = InetSocketAddress.createUnresolved(host, port);
        return this;
    }

    /**
     * Set remote server address.
     *
     * @param time Client socket timeout duration.
     * @param unit Client socket timeout duration measurement.
     * @return {@link ThriftClientBuilder} instance for chained call.
     *
     * @since 1.0
     */
    public final ThriftClientBuilder timeout(final long time, final TimeUnit unit) {
        timeout = time;
        timeoutUnit = unit;
        return this;
    }

    /**
     * Pass in a transport instance that is constructed outside of this builder.
     *
     * @param aTransport the {@link TTransport} instance that has been
     * instantiated outside.
     * @return {@link ThriftClientBuilder} instance for chained call.
     *
     * @since 1.0
     */
    public final ThriftClientBuilder reuseTransport(final TTransport aTransport) {
        this.transport = aTransport;
        return this;
    }

    /**
     * Define how to create a {@link TTransport} instance.
     * <p>Remember: DO NOT open it in this method.</p>
     *
     * @return The created {@link TTransport} instance.
     */
    protected abstract TTransport createTransport();

    /**
     * Define how to create a {@link TProtocol} instance.
     *
     * @param aTransport The transport instance used for create {@link
     * TProtocol} instance, which may be the one instantiated inside this
     * builder or outside this builder.
     * @param ifaceType The generated "Iface" interface {@link Class} object.
     * @return The created {@link TProtocol} instance.
     */
    protected abstract TProtocol createProtocol(TTransport aTransport,
            Class<?> ifaceType);

    /**
     * Try to find out the thrift generated "Iface" type and return a {@link
     * Class} object of this type.
     *
     * @param ifaceClientType The thrift generated "Client" type.
     * @return A {@link Class} object represent thrift generated "Iface" on
     * sucess, else return {@code null}.
     */
    protected final Class<?> getIface(final Class<?> ifaceClientType) {
        if (ifaceClientType == null || !ifaceClientType.isMemberClass())
            return null;
        for (Class<?> iff : ifaceClientType.getInterfaces()) {
            if ("Iface".equals(iff.getSimpleName())
                    && iff.getEnclosingClass() == ifaceClientType
                            .getEnclosingClass())
                return iff;
        }
        return null;
    }

    /**
     * Create thrift client.
     *
     * @param protocol The {@link TProtocol} instance used to instantiate thrift
     * client.
     * @param ifaceClientType The thrift generated "Client" {@link Class}
     * object.
     * @param <T> The required thrift client type.
     * @return A thrift client object or may throw {@link RuntimeException} on
     * failed.
     */
    protected final <T extends TServiceClient> T createIfaceClient(
            final TProtocol protocol, final Class<T> ifaceClientType) {
        T instance = null;
        try {
            Constructor<T> ctor = ifaceClientType
                    .getConstructor(TProtocol.class);
            instance = ctor.newInstance(protocol);
        } catch (NoSuchMethodException | SecurityException
                | InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return instance;
    }

    /**
     * Build the thrift client instance.
     *
     * @param ifaceClientType The {@link Class} object of the requried client
     * type.
     * @param <T> The client type parameter.
     *
     * @return The required thrift client which has the same type of {@code
     * ifaceClientType}.
     * @see #buildAndOpen(Class)
     */
    public final <T extends TServiceClient> T build(final Class<T> ifaceClientType) {
        TTransport tt = transport == null ? createTransport() : transport;
        return createIfaceClient(createProtocol(tt, getIface(ifaceClientType)),
                ifaceClientType);
    }

    /**
     * Build the thrift client instance and open the transport channel.
     *
     * @param ifaceClientType The requried client type.
     * @param <T> The required client type parameter.
     *
     * @return The required thrift client which has the same type of {@code
     * ifaceClientType}.
     * @throws TTransportException On failed to open transport.
     * @see #build(Class)
     */
    public final <T extends TServiceClient> T buildAndOpen(final Class<T> ifaceClientType)
            throws TTransportException {
        TTransport tt = transport == null ? createTransport() : transport;
        tt.open();
        return createIfaceClient(createProtocol(tt, getIface(ifaceClientType)),
                ifaceClientType);
    }

    @Override
    public final String toString() {
        return "{server: " + serverAddress + ", timeout: " + timeout
                + ", timeoutUnit: " + timeoutUnit + "}";
    }

    /**
     * Create a blocking thrift client.
     * <p>
     * As thrift code generator will auto implement two kinds of client: the
     * synchronous one and the asynchronous one. "blocking" means the
     * synchronous version.
     * </p>
     *
     * @return {@link ThriftClientBuilder} instance for chained call.
     */
    public static ThriftClientBuilder blocking() {
        return new BlockingServerClientBuilder();
    }

    /**
     * Interal client builder for create a blocking thrift client.
     *
     * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
     * @version Wed May 20 18:04:46 2015 +0800
     * @since 1.0
     */
    private static final class BlockingServerClientBuilder extends
            ThriftClientBuilder {

        /**
         * {@inheritDoc}
         */
        @Override
        protected TTransport createTransport() {
            return new TFramedTransport(new TSocket(
                    getServerAddress().getHostString(),
                    getServerAddress().getPort(),
                    getTimeoutUnit() == null ? 0
                            : (int) getTimeoutUnit()
                            .toMillis(getTimeoutDuration())));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected TProtocol createProtocol(final TTransport transport,
                final Class<?> ifaceType) {
            return new TMultiplexedProtocol(new TBinaryProtocol(transport),
                    ifaceType.getName());
        }
    }
}

