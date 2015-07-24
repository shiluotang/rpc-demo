package org.sqg.mina;

import java.net.SocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockingClient extends Client {

    private BlockingQueue<Object> resultQueue;

    public BlockingClient(SocketAddress remoteServerAddress) {
        super(remoteServerAddress);
        resultQueue = new ArrayBlockingQueue<>(0xff);
    }

    @Override
    protected Object handleMessage(Object messageObj) {
        try {
            resultQueue.put(messageObj);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <U, V> V request(final U u) {
        send(u);
        try {
            return (V) resultQueue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
