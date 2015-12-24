package org.sqg.rpc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

public final class RpcResponse implements Externalizable {

    private UUID requestId;
    private Object result;
    private Throwable throwable;

    public RpcResponse() {
    }

    public RpcResponse(final UUID requestId, final Object result,
            final Throwable throwable) {
        this.requestId = requestId;
        this.result = result;
        this.throwable = throwable;
    }

    public void setRequestId(final UUID value) {
        requestId = value;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(final Object value) {
        result = value;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(final Throwable value) {
        throwable = value;
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeObject(requestId);
        out.writeObject(result);
        out.writeObject(throwable);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        requestId = (UUID) in.readObject();
        result = in.readObject();
        throwable = (Throwable) in.readObject();
    }
}
