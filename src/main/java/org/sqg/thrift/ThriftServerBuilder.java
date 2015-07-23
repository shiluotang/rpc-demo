package org.sqg.thrift;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a thrift server.
 *
 * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
 * @version Mon May 25 15:47:52 2015 +0800
 * @since 1.0
 */
public abstract class ThriftServerBuilder {

    /**
     * SLF4J logger.
     */
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ThriftServerBuilder.class);

    /**
     * The port that will be used by the created thrift server instance.
     */
    private int port;

    /**
     * The timeout duration that will be used by the created thrift server
     * instance.
     */
    private long timeout;

    /**
     * The timeout measurement that will be used by the created thrift server
     * instance.
     */
    private TimeUnit timeoutUnit;

    /**
     * Store all the possible {@link TBaseProcessor} object that can be created
     * from the corresponding class.
     */
    private Map<Class<?>, TBaseProcessor<?>> processors;

    /**
     * Construct root server builder instance.
     *
     * @since 1.0
     */
    public ThriftServerBuilder() {
        processors = new HashMap<>();
    }

    /**
     * Get the port of the server created by this builder will be bound.
     *
     * @return The port of the server created by this builder will be bound.
     *
     * @since 1.0
     */
    public final int getPort() {
        return port;
    }

    /**
     * Get the timeout duration of the server socket option created by this
     * builder.
     *
     * @return The timeout duration of the server socket option created by this
     * builder.
     * @since 1.0
     * @see #getTimeoutUnit
     */
    public final long getTimeoutDuration() {
        return timeout;
    }

    /**
     * Get the timeout duration measurement.
     *
     * @return The timeout duration measurement.
     * @since 1.0
     * @see #getTimeoutDuration
     */
    public final TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    /**
     * Get the already created and mapped {@link TBaseProcessor}s.
     *
     * @return The already created and mapped {@link TBaseProcessor}s.
     * @since 1.0
     */
    public final Map<Class<?>, TBaseProcessor<?>> getMappedProcessors() {
        return processors;
    }

    /**
     * Set thrift server listen port.
     *
     * @param aPort The port that will be used by the created thrift server
     * instance.
     * @return {@link ThriftServerBuilder} instance for chained call.
     */
    public final ThriftServerBuilder listen(final int aPort) {
        this.port = aPort;
        return this;
    }

    /**
     * Set thrift server socket timeout.
     *
     * @param time timeout duration.
     * @param unit timeout duration measurement.
     * @return {@link ThriftServerBuilder} instance for chained call.
     */
    public final ThriftServerBuilder timeout(final long time,
            final TimeUnit unit) {
        timeout = time;
        timeoutUnit = unit;
        return this;
    }

    /**
     * Create {@link TBaseProcessor} instance from object implemented "Iface".
     *
     * @param ifaceImplementor The object that has implemented "Iface".
     * @param ifaceType The "Iface" type that has been implemented.
     * @param <T> The requested "Iface" type parameter type.
     * @return The created {@link TBaseProcessor} instance on success, otherwise
     * return {@code null}.
     */
    private <T> TBaseProcessor<?> createProcessor(
            final T ifaceImplementor, final Class<?> ifaceType) {
        if (ifaceType == null || !ifaceType.isMemberClass()
                || !"Iface".equals(ifaceType.getSimpleName()))
            return null;
        Class<?> processorClass = null;
        TBaseProcessor<?> processor = null;
        Class<?> enclosingClass = ifaceType.getEnclosingClass();
        for (Class<?> declaredClass : enclosingClass.getDeclaredClasses()) {
            if (TBaseProcessor.class.isAssignableFrom(declaredClass)) {
                processorClass = declaredClass;
                break;
            }
        }
        if (processorClass == null)
            return null;
        Constructor<?> ctor;
        try {
            ctor = processorClass.getConstructor(ifaceType);
            processor = (TBaseProcessor<?>) ctor.newInstance(ifaceImplementor);
        } catch (NoSuchMethodException | SecurityException
                | InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            LOGGER.warn(e.getMessage(), e);
        }
        return processor;
    }

    /**
     * Create all possible {@link TBaseProcessor} instances from the provided
     * {@code ifaceImplementor} Object.
     *
     * @param ifaceImplementor The object that has implemented some "Iface"s.
     * @param <T> The type parameter of {@code ifaceImplementor}.
     * @return A {@link Map} contains the {@link TBaseProcessor} and their
     * corresponding "Iface" type.
     */
    private <T> Map<Class<?>, TBaseProcessor<?>> createProcessors(
            final T ifaceImplementor) {
        Map<Class<?>, TBaseProcessor<?>> createdProcessors = new HashMap<>();
        TBaseProcessor<?> createdProcessor = null;
        for (Class<?> iff : ifaceImplementor.getClass().getInterfaces()) {
            createdProcessor = createProcessor(ifaceImplementor, iff);
            if (createdProcessor != null)
                createdProcessors.put(iff, createdProcessor);
        }
        return createdProcessors;
    }

    /**
     * Create {@link TBaseProcessor} from the object that has implemented the
     * requested "Iface" interface.
     *
     * @param ifaceImplementor The object that has implemented the "Iface"
     * interface which is denoted by the {@code ifaceType} parameter.
     * @param ifaceType The "Iface" interface that {@code ifaceImplementor} has
     * implemented.
     * @param <T> The type parameter of {@code ifaceImplementor}.
     * @param <Iface> The type parameter represent "Iface".
     * @return {@link ThriftServerBuilder} instance for chained call.
     * @throws RuntimeException On failed to create {@link TBaseProcessor}.
     * @since 1.0
     */
    public final <T extends Iface, Iface> ThriftServerBuilder addProcessor(
            final T ifaceImplementor, final Class<Iface> ifaceType) {
        TBaseProcessor<?> processor = createProcessor(ifaceImplementor,
                ifaceType);
        if (processor == null)
            throw new RuntimeException(
                    "Can't create processor from implementor");
        processors.put(ifaceType, processor);
        return this;
    }

    /**
     * Create all possible {@link TBaseProcessor} instances and store them for
     * future step.
     * <p>
     * If the {@code ifaceImplementor} has implemented more than one "Iface"
     * interfaces, then the {@link TBaseProcessor} to be created will be more
     * than one too.
     * </p>
     *
     * @param ifaceImplementor The object which has implemented "Iface"
     * @param <T> The type parameter of {@code ifaceImplementor}.
     * @return {@link ThriftServerBuilder} instance for chained call.
     * interfaces.
     *
     * @since 1.0
     */
    public final <T> ThriftServerBuilder addAllImplementedProcessors(
            final T ifaceImplementor) {
        Map<Class<?>, TBaseProcessor<?>> createdProcessors =
            createProcessors(ifaceImplementor);
        if (createdProcessors.size() < 1)
            throw new RuntimeException(
                    "Can't create processor from implementor");
        processors.putAll(createProcessors(ifaceImplementor));
        return this;
    }

    /**
     * The last step of create a thrift server.
     * Make sure all neccessary settings has already be set.
     *
     * @return The created thrift server instance.
     */
    public abstract TServer build();

    /**
     * Construct a {@link ThriftServerBuilder} that can build a {@link
     * TNonblockingServer}.
     *
     * @return The {@link ThriftServerBuilder} instance for chained call
     * @since 1.0
     */
    public static ThriftServerBuilder nonblocking() {
        return new NonblockingServerBuilder();
    }

    /**
     * Construct a {@link ThriftServerBuilder} that can build a {@link
     * TThreadPoolServer}.
     *
     * @return The {@link ThriftServerBuilder} instance for chained call
     * @since 1.0
     */
    public static ThriftServerBuilder threadPool() {
        return new ThreadPoolServerBuilder();
    }

    /**
     * Construct a {@link ThriftServerBuilder} that can build a {@link
     * TThreadedSelectorServer}.
     *
     * @return The {@link ThriftServerBuilder} instance for chained call
     * @since 1.0
     */
    public static ThriftServerBuilder threadedSelector() {
        return new ThreadedSelectorServerBuilder();
    }

    /**
     * The subclass of {@link ThriftServerBuilder} for constructing {@link
     * TNonblockingServer}.
     *
     * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
     * @version Mon May 25 15:47:52 2015 +0800
     * @since 1.0
     */
    private static final class NonblockingServerBuilder extends ThriftServerBuilder {

        /**
         * Prevent 16M problem of {@link TNonblockingServer}.
         */
        private static final int MAX_READ_BYTES = 16383928;

        @Override
        public TServer build() {
            TMultiplexedProcessor processor = new TMultiplexedProcessor();
            for (Map.Entry<Class<?>, TBaseProcessor<?>> entry
                    : getMappedProcessors().entrySet())
                processor.registerProcessor(entry.getKey().getName(),
                        entry.getValue());
            try {
                TNonblockingServerSocket transport = new TNonblockingServerSocket(
                        getPort(), getTimeoutUnit() == null ? 0
                                : (int) getTimeoutUnit()
                                .toMillis(getTimeoutDuration()));
                TNonblockingServer.Args args = new TNonblockingServer.Args(
                        transport);
                args.processor(processor);
                args.maxReadBufferBytes = MAX_READ_BYTES;
                TNonblockingServer server = new TNonblockingServer(args);
                return server;
            } catch (TTransportException e) {
                throw new RuntimeException(e);
            }
        }
    }

     /**
     * The subclass of {@link ThriftServerBuilder} for constructing
     * {@link TThreadPoolServer}.
     *
     * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
     * @version Mon May 25 15:47:52 2015 +0800
     * @since 1.0
     */
    private static final class ThreadPoolServerBuilder extends ThriftServerBuilder {

        @Override
        public TServer build() {
            TMultiplexedProcessor processor = new TMultiplexedProcessor();
            for (Map.Entry<Class<?>, TBaseProcessor<?>> entry
                    : getMappedProcessors().entrySet())
                processor.registerProcessor(entry.getKey().getName(),
                        entry.getValue());
            try {
                TServerTransport transport = new TServerSocket(getPort(),
                        getTimeoutUnit() == null ? 0
                                : (int) getTimeoutUnit()
                                .toMillis(getTimeoutDuration()));
                TThreadPoolServer.Args args = new TThreadPoolServer.Args(
                        transport);
                args.processor(processor);
                TThreadPoolServer server = new TThreadPoolServer(args);
                return server;
            } catch (TTransportException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * The subclass of {@link ThriftServerBuilder} for constructing
     * {@link TThreadedSelectorServer}.
     *
     * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
     * @version Mon May 25 15:47:52 2015 +0800
     * @since 1.0
     */
    private static final class ThreadedSelectorServerBuilder extends
            ThriftServerBuilder {

        @Override
        public TServer build() {
            TMultiplexedProcessor processor = new TMultiplexedProcessor();
            for (Map.Entry<Class<?>, TBaseProcessor<?>> entry
                    : getMappedProcessors().entrySet())
                processor.registerProcessor(entry.getKey().getName(),
                        entry.getValue());
            try {
                TNonblockingServerSocket transport = new TNonblockingServerSocket(
                        getPort(), getTimeoutUnit() == null ? 0
                                : (int) getTimeoutUnit()
                                .toMillis(getTimeoutDuration()));
                TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(
                        transport);
                args.processor(processor);
                TThreadedSelectorServer server = new TThreadedSelectorServer(
                        args);
                return server;
            } catch (TTransportException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
