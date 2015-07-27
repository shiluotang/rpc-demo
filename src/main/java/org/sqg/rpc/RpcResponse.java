package org.sqg.rpc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public final class RpcResponse implements Externalizable {

    private Throwable throwable;
    private Object result;

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(final Throwable value) {
        throwable = value;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(final Object value) {
        result = value;
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeObject(throwable);
        out.writeObject(result);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        throwable = (Throwable) in.readObject();
        result = in.readObject();
    }
}
