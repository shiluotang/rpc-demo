package org.sqg.rpc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;

public final class RpcRequest implements Externalizable {

    private Class<?> iface;
    private String methodName;
    private Class<?>[] parameterTypes;
    private Object[] arguments;

    public RpcRequest() {

    }

    public RpcRequest(final Class<?> iface, final String methodName,
            final Class<?>[] parameterTypes, final Object[] arguments) {
        this.iface = iface;
        this.methodName = methodName;
        this.parameterTypes = parameterTypes;
        this.arguments = arguments;
    }

    public Class<?> getIface() {
        return iface;
    }

    public String getMethodName() {
        return methodName;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public Method getMethod() {
        try {
            return iface.getMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(iface);
        out.writeUTF(methodName);
        if (parameterTypes != null) {
            out.writeInt(parameterTypes.length);
            for (Class<?> parameterType : parameterTypes)
                out.writeObject(parameterType);
        } else
            out.writeInt(0);
        if (arguments != null) {
            out.writeInt(arguments.length);
            for (Object arg : arguments)
                out.writeObject(arg);
        } else
            out.writeInt(0);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        iface = (Class<?>) in.readObject();
        methodName = in.readUTF();
        parameterTypes = new Class<?>[in.readInt()];
        for (int i = 0, n = parameterTypes.length; i < n; ++i)
            parameterTypes[i] = (Class<?>) in.readObject();
        arguments = new Object[in.readInt()];
        for (int i = 0, n = arguments.length; i < n; ++i)
            arguments[i] = in.readObject();
    }
}
