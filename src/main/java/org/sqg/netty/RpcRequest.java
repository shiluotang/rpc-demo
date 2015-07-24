package org.sqg.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;

public class RpcRequest implements Serializable {

    private static final long serialVersionUID = 9094466951558223550L;

    private Class<?> iface;
    private String methodName;
    private Class<?>[] parameterTypes;
    private Object[] arguments;

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

    public static final class Decoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                List<Object> out) throws Exception {
            int size = 0;
            byte[] bytes = null;
            while (in.readableBytes() * 8 > Integer.SIZE) {
                in.markReaderIndex();
                size = in.readInt();
                if (in.readableBytes() < size) {
                    in.resetReaderIndex();
                    break;
                }
                bytes = new byte[size];
                in.readBytes(bytes);
                out.add(RpcServer.SERIALIZER.deserialize(bytes,
                        RpcRequest.class));
            }
        }
    }

    public static final class Encoder extends MessageToByteEncoder<RpcRequest> {

        @Override
        protected void encode(ChannelHandlerContext ctx, RpcRequest msg,
                ByteBuf out) throws Exception {
            byte[] bytes = RpcServer.SERIALIZER.serialize(msg);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }
    }
}
