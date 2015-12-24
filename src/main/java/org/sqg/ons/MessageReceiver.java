package org.sqg.ons;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.bean.ConsumerBean;
import com.aliyun.openservices.ons.api.bean.Subscription;

public abstract class MessageReceiver implements AutoCloseable, MessageListener {

    public static final String MESSAGEQUEUE_TYPE = "METAQ";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MessageReceiver.class);

    private volatile boolean receivedStopSignal;
    private volatile ConsumerBean bean;
    private final String consumerId;

    // --------------------------------
    // subscribe configuration.
    // --------------------------------
    private final String topic;
    private final Collection<String> interestedTags;

    public MessageReceiver(final String topic,
            final Collection<String> interestedTags) {
        try {
            this.topic = topic;
            this.interestedTags = interestedTags;
            this.bean = new ConsumerBean();
            this.consumerId = UUID.randomUUID().toString();
            this.receivedStopSignal = false;
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public MessageReceiver(final String topic, final String... interestedTags) {
        this(topic, Arrays.asList(interestedTags));
    }

    public synchronized void start() throws InterruptedException {
        if (bean != null) {
            Properties props = new Properties();
            props.setProperty(PropertyKeyConst.ConsumerId, consumerId);
            props.setProperty(PropertyKeyConst.MQType, MESSAGEQUEUE_TYPE);
            bean.setProperties(props);
            Map<Subscription, MessageListener> subscription = new HashMap<>();
            Subscription sub = new Subscription();
            sub.setTopic(topic);
            sub.setExpression(StringUtils.join(interestedTags, " || "));
            subscription.put(sub, this);
            bean.setSubscriptionTable(subscription);
            bean.start();
            bean.subscribe(topic, StringUtils.join(interestedTags, " || "),
                    this);
            while (!bean.isStarted() && !receivedStopSignal)
                TimeUnit.MILLISECONDS.sleep(10L);
        }
    }

    public boolean process(final Message message) {
        LOGGER.debug("[RECEIVED]: {}", message);
        if (!receivedStopSignal)
            return doProcess(message);
        return false;
    }

    public abstract boolean doProcess(Message message);

    public synchronized void stop() throws InterruptedException {
        receivedStopSignal = true;
        if (bean != null) {
            if (!bean.isClosed()) {
                try {
                    bean.unsubscribe(topic);
                } catch (UnsupportedOperationException ignore) {
                }
                bean.shutdown();
                while (!bean.isClosed())
                    TimeUnit.MILLISECONDS.sleep(10L);
                bean = null;
            }
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

    @Override
    public Action consume(Message message, ConsumeContext context) {
        try {
            if (process(message))
                return Action.CommitMessage;
            return Action.ReconsumeLater;
        } catch (Throwable t) {
            LOGGER.warn(t.getMessage(), t);
        }
        return Action.ReconsumeLater;
    }
}