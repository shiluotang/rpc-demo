package org.sqg.rpc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;

public final class RpcRequest implements Externalizable {

    private String ifaceName;
    private String methodName;
    private String[] parameterTypeNames;
    private Object[] arguments;

    public RpcRequest() {
    }

    public RpcRequest(final Class<?> iface, final String methodName,
            final Class<?>[] parameterTypes, final Object[] arguments) {
        this.ifaceName = iface.getName();
        this.methodName = methodName;
        this.parameterTypeNames = new String[parameterTypes != null ? parameterTypes.length
                : 0];
        for (int i = 0, n = parameterTypeNames.length; i < n; ++i)
            this.parameterTypeNames[i] = parameterTypes[i].getName();
        this.arguments = arguments;
    }

    public String getIfaceName() {
        return ifaceName;
    }

    public void setIfaceName(final String value) {
        ifaceName = value;
    }

    public String getMethodName() {
        return methodName;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public Method getMethod(final ClassLoader loader) {
        try {
            Class<?>[] parameterTypes = new Class<?>[parameterTypeNames.length];
            for (int i = 0, n = parameterTypes.length; i < n; ++i)
                parameterTypes[i] = Class.forName(parameterTypeNames[i], false,
                        loader);
            return Class.forName(ifaceName, false, loader).getMethod(
                    methodName, parameterTypes);
        } catch (NoSuchMethodException | SecurityException
                | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Method getMethod() {
        return getMethod(Thread.currentThread().getContextClassLoader());
    }

    protected Class<?>[] getParameterTypes(final ClassLoader loader)
            throws ClassNotFoundException {
        Class<?>[] parameterTypes = new Class<?>[parameterTypeNames.length];
        for (int i = 0, n = parameterTypes.length; i < n; ++i)
            parameterTypes[i] = Class.forName(parameterTypeNames[i], false,
                    loader);
        return parameterTypes;
    }

    public FastMethod getCglibFastMethod(final ClassLoader loader) {
        try {
            return FastClass.create(Class.forName(ifaceName)).getMethod(
                    methodName, getParameterTypes(loader));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public FastMethod getCglibFastMethod() {
        return getCglibFastMethod(Thread.currentThread()
                .getContextClassLoader());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(ifaceName);
        out.writeUTF(methodName);
        if (parameterTypeNames != null) {
            out.writeInt(parameterTypeNames.length);
            for (String parameterTypeName : parameterTypeNames)
                out.writeUTF(parameterTypeName);
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
        ifaceName = in.readUTF();
        methodName = in.readUTF();
        parameterTypeNames = new String[in.readInt()];
        for (int i = 0, n = parameterTypeNames.length; i < n; ++i)
            parameterTypeNames[i] = in.readUTF();
        arguments = new Object[in.readInt()];
        for (int i = 0, n = arguments.length; i < n; ++i)
            arguments[i] = in.readObject();
    }
}
