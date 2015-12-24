package org.sqg.ons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.rpc.RpcRequest;
import org.sqg.rpc.RpcRequestHandler;
import org.sqg.rpc.RpcResponse;

import com.aliyun.openservices.ons.api.Message;

public class RpcServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RpcServer.class);

    private final class Receiver extends MessageReceiver {

        public Receiver(String topic) {
            super(topic, new String[] { "rpc-request" });
        }

        @Override
        public boolean doProcess(final Message message) {
            if (receivedStopSignal)
                return false;
            RpcRequest request = null;
            try {
                request = MessageSender.SERIALIZER.deserialize(
                        message.getBody(), RpcRequest.class);
            } catch (Exception e) {
            }
            if (request != null) {
                Object servant = requestHandler.getServant(request);
                if (servant != null && !receivedStopSignal) {
                    RpcResponse response = requestHandler.processRequest(
                            request, servant);
                    if (response != null && !receivedStopSignal)
                        sender.send(response);
                    return true;
                }
            }
            return false;
        }
    }

    private volatile boolean receivedStopSignal;
    private RpcRequestHandler requestHandler;
    private volatile Receiver receiver;
    private volatile MessageSender sender;

    public RpcServer(final String topic, Object[] serviceImplementors) {
        try {
            this.receivedStopSignal = false;
            this.receiver = new Receiver(topic);
            this.sender = new MessageSender(topic, "rpc-response");
            this.requestHandler = new RpcRequestHandler(serviceImplementors);
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    public synchronized void start() throws InterruptedException {
        LOGGER.debug("RpcServer {} start start...", this);
        if (receiver != null)
            receiver.start();
        if (sender != null)
            sender.start();
        LOGGER.debug("RpcServer {} start stopped.", this);
    }

    public synchronized void stop() throws InterruptedException {
        LOGGER.debug("RpcServer {} stop start...", this);
        receivedStopSignal = true;
        if (receiver != null) {
            receiver.stop();
            receiver.close();
            receiver = null;
        }
        if (sender != null) {
            sender.stop();
            sender.close();
            sender = null;
        }
        LOGGER.debug("RpcServer {} stop stopped.", this);
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
