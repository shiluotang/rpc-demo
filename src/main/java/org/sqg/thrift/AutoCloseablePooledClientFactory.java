package org.sqg.thrift;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory create AutoCloseable and pooled thrift client.
 *
 * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
 * @version 2015/7/6 19:45
 * @since 1.0
 */
public final class AutoCloseablePooledClientFactory {

    /**
     * SLF4J Logger.
     */
    @SuppressWarnings("unused")
    private static final Logger LOGGER = LoggerFactory
            .getLogger(AutoCloseablePooledClientFactory.class);

    /**
     * Acting as interceptor for different methods.
     *
     * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
     * @version 2015/7/6 19:52
     * @since 1.0
     */
    private static final class AutoCloseableClientInvocationHandler<WrappedClient extends AutoCloseable, TClient extends TServiceClient>
            implements InvocationHandler {

        /**
         * Create pooled thrift client.
         *
         * @author <a href="mailto:quangang.sheng@adchina.com">Quangang
         *         Sheng</a>
         * @version 2015/7/6 19:52
         * @since 1.0
         */
        private final class PooledClientFactory extends
                BasePooledObjectFactory<TClient> {

            /**
             * The builder knows how to create a thrift client, including
             * timeout, remote server address.
             */
            private final ThriftClientBuilder builder;

            /**
             * Construct an instance of PooledClientFactory.
             *
             * @param aBuilder
             *            The pre-initialized thrift client builder object.
             * @since 1.0
             */
            public PooledClientFactory(final ThriftClientBuilder aBuilder) {
                this.builder = aBuilder;
            }

            @Override
            public TClient create() throws Exception {
                return builder.buildAndOpen(clientClass);
            }

            @Override
            public PooledObject<TClient> wrap(final TClient obj) {
                return new DefaultPooledObject<TClient>(obj);
            }

            @Override
            public void destroyObject(final PooledObject<TClient> p)
                    throws Exception {
                TClient client = p.getObject();
                TTransport transport = client.getInputProtocol().getTransport();
                if (transport.isOpen())
                    transport.close();
                super.destroyObject(p);
            }
        }

        /**
         * The thrift generated client class.
         */
        private final Class<TClient> clientClass;
        /**
         * The thrift IDL defined service interface.
         */
        private final Class<?> ifaceClass;
        /**
         * The method defined inside wrapped client.
         */
        private final Method closeMethod;
        /**
         * The pool for thrift client instances.
         */
        private ObjectPool<TClient> pool;

        /**
         * Construct AutoCloseableClientInvocationHandler instance.
         * <p>
         * {@link closeMethod} will be lookup from the derivation chain from the
         * most derived subclass to the root class.
         * </p>
         *
         * @param builder
         *            The thrift client builder which can contains the remote
         *            server address and timeout information.
         * @param wrappedClientClass
         *            The combination of AutoCloseable and Iface.
         * @param aClientClass
         *            The thrift generated client type.
         *
         * @since 1.0
         */
        public AutoCloseableClientInvocationHandler(
                final ThriftClientBuilder builder,
                final Class<WrappedClient> wrappedClientClass,
                final Class<TClient> aClientClass) {
            this.clientClass = aClientClass;
            this.ifaceClass = Objects
                    .requireNonNull(getIfaceClass(clientClass));
            try {
                this.closeMethod = wrappedClientClass.getMethod("close");
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
            this.pool = new GenericObjectPool<>(
                    new PooledClientFactory(builder));
        }

        /**
         * Try to get Iface class which is implemented by the thrift generated
         * client type.
         *
         * @param ifaceClientType
         *            The iface client type which is generated by thrift source
         *            code generator.
         * @return return the Iface {@link java.lang.Class} object on found,
         *         otherwise return {@code null}.
         */
        private static Class<?> getIfaceClass(final Class<?> ifaceClientType) {
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
         * {@inheritDoc}
         */
        @Override
        public Object invoke(final Object proxy, final Method method,
                final Object[] args) throws Throwable {
            final Class<?> declaringClass = method.getDeclaringClass();
            if (declaringClass == ifaceClass
                    || declaringClass == TServiceClient.class
                    || declaringClass == clientClass) {
                TClient client = null;
                try {
                    client = pool.borrowObject();
                    try {
                        return method.invoke(client, args);
                    } catch (Exception e) {
                        pool.invalidateObject(client);
                        client = null;
                        throw e;
                    } finally {
                        if (client != null) {
                            pool.returnObject(client);
                        }
                        client = null;
                    }
                } catch (Exception e) {
                    throw e;
                }
            } else if (declaringClass == AutoCloseable.class
                    || declaringClass == Closeable.class
                    || closeMethod.equals(method)) {
                pool.close();
                pool.clear();
                return null;
            } else
                throw new UnsupportedOperationException("method \"" + method
                        + "\" is not supported!");
        }
    }

    /**
     * Make checkstyle plugin happy.
     */
    private AutoCloseablePooledClientFactory() {
    }

    /**
     * Construct a new instance of {@code WrappedClient}.
     *
     * @param builder
     *            The thrift client builder which can contains the remote server
     *            address and timeout information.
     * @param wrappedClientClass
     *            The combination of {@link AutoCloseable} and Iface.
     * @param clientClass
     *            The thrift generated client type.
     *
     * @param <WrappedClient>
     *            The combination of thrift IDL {@code Iface} and
     *            {@link AutoCloseable}.
     * @param <TClient>
     *            The thrift generated client type.
     *
     * @return The proxied object of {@code WrappedClient}.
     *
     * @since 1.0
     */
    public static <WrappedClient extends AutoCloseable, TClient extends TServiceClient> WrappedClient newInstance(
            final ThriftClientBuilder builder,
            final Class<WrappedClient> wrappedClientClass,
            final Class<TClient> clientClass) {
        return wrappedClientClass.cast(Proxy.newProxyInstance(Thread
                .currentThread().getContextClassLoader(),
                new Class<?>[] { wrappedClientClass },
                new AutoCloseableClientInvocationHandler<>(builder,
                        wrappedClientClass, clientClass)));
    }
}
