package org.sqg.rpc;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class RpcRequestHandler {

    private ConcurrentMap<String, Object> services;

    public RpcRequestHandler() {
        this(null);
    }

    public RpcRequestHandler(final Object[] serviceImplementors) {
        services = new ConcurrentHashMap<>();
        if (serviceImplementors != null)
            for (Object serviceImplementor : serviceImplementors)
                if (serviceImplementor != null)
                    addService(serviceImplementor);
    }

    public Object getServant(final RpcRequest request) {
        return services.get(request.getIfaceName());
    }

    public void addService(final Object service) {
        if (service != null)
            for (Class<?> iface : getServiceIfaces(service))
                addService(service, iface);
    }

    private Collection<Class<?>> getServiceIfaces(
            final Object serviceImplementor) {
        if (serviceImplementor == null)
            throw new NullPointerException();
        Collection<Class<?>> ifaces = new ArrayList<Class<?>>();
        for (Class<?> iface : serviceImplementor.getClass().getInterfaces()) {
            if (RpcService.class.isAssignableFrom(iface)
                    && !RpcService.class.equals(iface)
                    || iface.getAnnotation(RpcContract.class) != null) {
                ifaces.add(iface);
            }
        }
        return ifaces;
    }

    private void addService(final Object service, final Class<?> serviceIface) {
        if (service == null || serviceIface == null)
            return;
        services.put(serviceIface.getName(), service);
    }

    public RpcResponse processRequest(final RpcRequest request,
            final Object servant) {
        RpcResponse response = new RpcResponse();
        response.setRequestId(request.getRequestId());
        try {
            response.setResult(request.getMethod().invoke(servant,
                    request.getArguments()));
        } catch (IllegalArgumentException | InvocationTargetException
                | IllegalAccessException e) {
            response.setThrowable(e);
        }
        return response;
    }
}
