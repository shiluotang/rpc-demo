package org.sqg.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Sharable
public class RpcRequestHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private ConcurrentMap<Class<?>, Object> services;

    public RpcRequestHandler() {
        this(null);
    }

    public RpcRequestHandler(final RpcService[] serviceImplementors) {
        services = new ConcurrentHashMap<Class<?>, Object>();
        if (services != null)
            for (RpcService serviceImplementor : serviceImplementors)
                if (serviceImplementor != null)
                    addService(serviceImplementor);
    }

    public RpcRequestHandler(final Object[] serviceImplementors) {
        services = new ConcurrentHashMap<Class<?>, Object>();
        if (serviceImplementors != null)
            for (Object serviceImplementor : serviceImplementors)
                if (serviceImplementor != null)
                    addService(serviceImplementor);
    }

    private Object getServant(final RpcRequest request) {
        return services.get(request.getIface());
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
        services.put(serviceIface, service);
    }

    private RpcResponse processRequest(final RpcRequest request,
            final Object servant) {
        RpcResponse response = new RpcResponse();
        try {
            response.setResult(request.getMethod().invoke(servant,
                    request.getArguments()));
        } catch (IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            response.setThrowable(e);
        }
        return response;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx,
            final RpcRequest msg) throws Exception {
        Object servant = getServant(msg);
        if (servant == null)
            return;
        ctx.writeAndFlush(processRequest(msg, servant)).sync();
    }

}
