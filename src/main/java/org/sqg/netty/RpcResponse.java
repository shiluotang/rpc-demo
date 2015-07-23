package org.sqg.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

public class RpcResponse {

    private Throwable throwable;
    private Object result;

    public static final class Decoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                List<Object> out) throws Exception {
            int size = 0;
            byte[] bytes = null;
            while (in.readableBytes() > Integer.BYTES) {
                in.markReaderIndex();
                size = in.readInt();
                if (in.readableBytes() < size) {
                    in.resetReaderIndex();
                    break;
                }
                bytes = new byte[size];
                in.readBytes(bytes);
                out.add(RpcService.SERIALIZER.deserialize(bytes,
                        RpcResponse.class));
            }
        }
    }

    public static final class Encoder extends MessageToByteEncoder<RpcResponse> {

        @Override
        protected void encode(ChannelHandlerContext ctx, RpcResponse msg,
                ByteBuf out) throws Exception {
            byte[] bytes = RpcService.SERIALIZER.serialize(msg);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }
    }
}
