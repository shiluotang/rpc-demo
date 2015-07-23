package org.sqg.mina;

import java.net.SocketAddress;
import java.util.concurrent.Semaphore;

public class BlockingClient extends Client {

    private Semaphore resultWaiter;
    private Object response;

    public BlockingClient(SocketAddress remoteServerAddress) {
        super(remoteServerAddress);
        resultWaiter = new Semaphore(1);
    }

    @Override
    protected Object handleMessage(Object messageObj) {
        response = messageObj;
        resultWaiter.release();
        return null;
    }

    @SuppressWarnings("unchecked")
    public <U, V> V request(final U u) {
        try {
            resultWaiter.acquire();
            send(u);
            resultWaiter.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        V v = (V) response;
        response = null;
        resultWaiter.release();
        return v;
    }
}
