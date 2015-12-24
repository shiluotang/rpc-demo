package org.sqg.ons;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.util.JdkSerializer;
import org.sqg.util.Serializer;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.bean.ProducerBean;

public class MessageSender implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MessageSender.class);

    public static final Serializer SERIALIZER = new JdkSerializer();

    private volatile boolean receivedStopSignal;
    private ProducerBean bean;
    private final String topic;
    private final String tag;
    private final String producerId;

    public MessageSender(final String topic, final String tag) {
        try {
            this.producerId = UUID.randomUUID().toString();
            this.receivedStopSignal = false;
            this.topic = topic;
            this.tag = tag;
            this.bean = new ProducerBean();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    public synchronized void start() throws InterruptedException {
        if (bean != null) {
            Properties props = new Properties();
            props.setProperty(PropertyKeyConst.ProducerId, producerId);
            props.setProperty(PropertyKeyConst.MQType,
                    MessageReceiver.MESSAGEQUEUE_TYPE);
            bean.setProperties(props);
            bean.start();
            while (!bean.isStarted() && !receivedStopSignal)
                TimeUnit.MILLISECONDS.sleep(10L);
        }
    }

    public synchronized void stop() throws InterruptedException {
        receivedStopSignal = true;
        if (bean != null) {
            if (!bean.isClosed()) {
                bean.shutdown();
                while (!bean.isClosed())
                    TimeUnit.MILLISECONDS.sleep(10L);
                bean = null;
            }
        }
    }

    public <T> void send(final T message) {
        try {
            SendResult result = bean.send(new Message(topic, tag, SERIALIZER
                    .serialize(message)));
            LOGGER.debug("message {} has been sent.", result.getMessageId());
        } catch (Exception e) {
            if (!receivedStopSignal)
                LOGGER.warn(e.getMessage(), e);
        }
    }

    public <T> void sendOneway(final T message) {
        try {
            bean.sendOneway(new Message(topic, tag, SERIALIZER
                    .serialize(message)));
        } catch (Exception e) {
            if (!receivedStopSignal)
                LOGGER.warn(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}