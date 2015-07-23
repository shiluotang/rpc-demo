package org.sqg.mina;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.service.IoService;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;
import org.apache.mina.filter.codec.demux.MessageDecoderAdapter;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;
import org.apache.mina.filter.codec.demux.MessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.util.JdkSerializer;
import org.sqg.util.Serializer;

public abstract class MultipartIoService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MultipartIoService.class.getName());

    private static class MultipartDemuxingProtocolCodecFactory extends
            DemuxingProtocolCodecFactory {

        private static final class Decoder extends MessageDecoderAdapter {

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
                byte[] buf = new byte[in.getInt()];
                in.get(buf);
                out.write(ByteBuffer.wrap(buf));
                return MessageDecoderResult.OK;
            }

        }

        private static final class Encoder implements
                MessageEncoder<ByteBuffer> {

            @Override
            public void encode(IoSession session, ByteBuffer message,
                    ProtocolEncoderOutput out) throws Exception {
                IoBuffer buf = IoBuffer.allocate(message.remaining() + 4);
                buf.putInt(message.remaining());
                buf.put(message);
                buf.flip();
                out.write(buf);
            }

        }

        public MultipartDemuxingProtocolCodecFactory() {
            super.addMessageDecoder(new Decoder());
            super.addMessageEncoder(ByteBuffer.class, new Encoder());
        }
    }

    private final class Handler extends IoHandlerAdapter {

        @Override
        public void messageReceived(IoSession session, Object message)
                throws Exception {
            Object request = MultipartIoService.this
                    .decodeMessage((ByteBuffer) message);
            Object response = MultipartIoService.this.handleMessage(request);
            if (response != null)
                session.write(MultipartIoService.this.encodeMessage(response));
        }

        @Override
        public void messageSent(IoSession session, Object message)
                throws Exception {
            ((ByteBuffer) message).flip();
            LOGGER.debug("SENT {}", message);
        }

        @Override
        public void exceptionCaught(IoSession session, Throwable cause)
                throws Exception {
            super.exceptionCaught(session, cause);
        }
    }

    protected IoService service;
    protected final Serializer serializer = new JdkSerializer();

    /**
     * Only create service add codec and set handler.
     */
    protected void initService() {
        service = createIoService();
        service.getFilterChain().addLast(
                "demux-codec",
                new ProtocolCodecFilter(
                        new MultipartDemuxingProtocolCodecFactory()));
        service.setHandler(new Handler());
    }

    protected abstract IoService createIoService();

    /**
     * Dispose the service.
     */
    @Override
    public void close() {
        if (service != null)
            service.dispose();
    }

    protected <T> T decodeMessage(ByteBuffer message, Class<T> type) {
        ByteArrayInputStream is = null;
        if (message.hasArray())
            is = new ByteArrayInputStream(message.array(),
                    message.arrayOffset() + message.position(),
                    message.remaining());
        else {
            byte[] data = new byte[message.remaining()];
            message.get(data);
            is = new ByteArrayInputStream(data);
        }

        try {
            return serializer.deserialize(is, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private Object decodeMessage(ByteBuffer message) {
        ByteArrayInputStream is = null;
        if (message.hasArray()) {
            is = new ByteArrayInputStream(message.array(),
                    message.arrayOffset() + message.position(),
                    message.remaining());
        } else {
            byte[] data = new byte[message.remaining()];
            message.get(data);
            is = new ByteArrayInputStream(data);
        }

        try {
            return serializer.deserialize(is);
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }

    }

    private ByteBuffer encodeMessage(Object messageObj) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            serializer.serialize(messageObj, os);
            return ByteBuffer.wrap(os.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * handle message and create response object.
     *
     * @param messageObj
     * @return response object if response is neccessary. Otherwise null.
     */
    protected abstract Object handleMessage(Object messageObj);

    protected <T> void sendMessage(IoSession session, T typedMessage) {
        if (session != null)
            session.write(encodeMessage(typedMessage));
    }
}
