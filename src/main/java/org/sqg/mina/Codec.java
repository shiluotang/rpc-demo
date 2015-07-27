package org.sqg.mina;

import java.nio.ByteBuffer;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;
import org.apache.mina.filter.codec.demux.MessageDecoderAdapter;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;
import org.apache.mina.filter.codec.demux.MessageEncoder;
import org.sqg.util.JdkSerializer;
import org.sqg.util.Serializer;

public final class Codec<U, V> extends DemuxingProtocolCodecFactory {

    private static final Serializer SERIALIZER = new JdkSerializer();

    private final class Decoder extends MessageDecoderAdapter {

        public MessageDecoderResult decodable(IoSession session, IoBuffer in) {
            try {
                return !in.prefixedDataAvailable(4) ? MessageDecoderResult.NEED_DATA
                        : MessageDecoderResult.OK;
            } catch (Exception e) {
                return MessageDecoderResult.NOT_OK;
            }
        }

        /**
         * Decode IoBuffer into {@code ByteBuffer}.
         *
         * @see ByteBuffer
         */
        public MessageDecoderResult decode(IoSession session, IoBuffer in,
                ProtocolDecoderOutput out) throws Exception {
            in.mark();
            byte[] buf = new byte[in.getInt()];
            in.get(buf);
            try {
                out.write(SERIALIZER.deserialize(buf, inboundType));
                return MessageDecoderResult.OK;
            } catch (Exception e) {
                in.reset();
                return MessageDecoderResult.NOT_OK;
            }
        }

    }

    private final class Encoder implements MessageEncoder<V> {

        @Override
        public void encode(final IoSession session, final V message,
                final ProtocolEncoderOutput out) throws Exception {
            if (outboundType.isInstance(message)) {
                byte[] bytes = SERIALIZER.serialize(message);
                IoBuffer buf = IoBuffer.allocate(bytes.length + 4);
                buf.putInt(bytes.length);
                buf.put(bytes);
                buf.flip();
                out.write(buf);
            }
        }

    }

    private Class<U> inboundType;
    private Class<V> outboundType;

    public Codec(final Class<U> inboundType, final Class<V> outboundType) {
        this.inboundType = inboundType;
        this.outboundType = outboundType;
        super.addMessageDecoder(new Decoder());
        super.addMessageEncoder(outboundType, new Encoder());
    }
}
