package org.sqg.rpc;

public abstract class RpcClient implements AutoCloseable {

    protected abstract RpcResponse doRPC(RpcRequest request);

    @Override
    public abstract void close();
}
