package org.sqg.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

import org.sqg.util.KryoSerializer;
import org.sqg.util.Serializer;

public final class Codec<U, V> {

    private static final Serializer SERIALIZER = new KryoSerializer();

    private final class Encoder extends MessageToByteEncoder<U> {

        @Override
        public boolean acceptOutboundMessage(Object msg) throws Exception {
            return outboundType.isInstance(msg);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, U msg, ByteBuf out)
                throws Exception {
            byte[] bytes = SERIALIZER.serialize(msg);
            out.ensureWritable(bytes.length + 4, true);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }
    }

    private final class Decoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                List<Object> out) throws Exception {
            if (in.readableBytes() < 4)
                return;
            in.markReaderIndex();
            int size = in.readInt();
            if (size <= 0 || in.readableBytes() < size) {
                in.resetReaderIndex();
                return;
            }
            byte[] bytes = new byte[size];
            in.readBytes(bytes);
            try {
                out.add(SERIALIZER.deserialize(bytes, inboundType));
            } catch (ClassCastException e) {
                in.resetReaderIndex();
            }
        }
    }

    private final Class<U> inboundType;
    private final Class<V> outboundType;

    public Codec(final Class<U> inboundType, final Class<V> outboundType) {
        this.inboundType = inboundType;
        this.outboundType = outboundType;
    }

    public MessageToByteEncoder<U> getEncoder() {
        return new Encoder();
    }

    public ByteToMessageDecoder getDecoder() {
        return new Decoder();
    }
}
