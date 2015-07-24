package org.sqg.netty;

import java.io.Serializable;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

public class RpcResponse implements Serializable {

    private static final long serialVersionUID = 1628980768995501430L;

    private Throwable throwable;
    private Object result;

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(final Throwable value) {
        throwable = value;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(final Object value) {
        result = value;
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
                        RpcResponse.class));
            }
        }
    }

    public static final class Encoder extends MessageToByteEncoder<RpcResponse> {

        @Override
        protected void encode(ChannelHandlerContext ctx, RpcResponse msg,
                ByteBuf out) throws Exception {
            byte[] bytes = RpcServer.SERIALIZER.serialize(msg);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }
    }
}
