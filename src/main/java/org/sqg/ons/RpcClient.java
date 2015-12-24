package org.sqg.ons;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.rpc.RpcRequest;
import org.sqg.rpc.RpcResponse;

import com.aliyun.openservices.ons.api.Message;

public class RpcClient extends org.sqg.rpc.RpcClient {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RpcClient.class);

    private final class Receiver extends MessageReceiver {

        public Receiver(final String topic) {
            super(topic, "rpc-response");
        }

        @Override
        public boolean doProcess(Message message) {
            RpcResponse response = null;
            try {
                response = MessageSender.SERIALIZER.deserialize(
                        message.getBody(), RpcResponse.class);
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
                return false;
            }
            TransferQueue<RpcResponse> queue = results.remove(response
                    .getRequestId());
            if (queue == null)
                return false;
            queue.offer(response);
            return true;
        }
    }

    private volatile boolean receivedStopSignal;
    private MessageSender sender;
    private Receiver receiver;
    private final ConcurrentMap<UUID, TransferQueue<RpcResponse>> results;

    public RpcClient(final String topic) {
        try {
            this.results = new ConcurrentHashMap<UUID, TransferQueue<RpcResponse>>();
            this.sender = new MessageSender(topic, "rpc-request");
            this.receiver = new Receiver(topic);
            this.receivedStopSignal = false;
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    public void start() throws InterruptedException {
        if (receiver != null)
            receiver.start();
        if (sender != null)
            sender.start();
    }

    @Override
    protected RpcResponse doRPC(final RpcRequest request) {
        if (receivedStopSignal)
            return null;
        TransferQueue<RpcResponse> queue = new LinkedTransferQueue<>();
        results.put(request.getRequestId(), queue);
        sender.send(request);
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    public void stop() throws InterruptedException {
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
