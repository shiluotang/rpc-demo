package org.sqg.netty;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

public final class RpcProxy {

    private static final class RpcInvocationHandler implements
            InvocationHandler {

        private RpcClient client;

        public RpcInvocationHandler(final RpcClient client) {
            this.client = Objects.requireNonNull(client);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            RpcResponse response = null;
            response = client.doRpcCall(new RpcRequest(method
                    .getDeclaringClass(), method.getName(), method
                    .getParameterTypes(), args));
            Throwable t = null;
            if ((t = response.getThrowable()) != null)
                throw t;
            return response.getResult();
        }
    }

    public static <T> T newProxyInstance(final RpcClient client,
            final Class<T> iface) {
        return iface.cast(Proxy.newProxyInstance(Thread.currentThread()
                .getContextClassLoader(), new Class<?>[] { iface },
                new RpcInvocationHandler(client)));
    }
}
