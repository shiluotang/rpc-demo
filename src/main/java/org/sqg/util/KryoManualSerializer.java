package org.sqg.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class KryoManualSerializer extends AbstractSerializer {

    /**
     * FIXME How to prevent memory leaks here?
     */
    private static final ThreadLocal<Kryo> KRYOS = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            return new Kryo();
        };
    };

    @Override
    public <T> void serialize(T objectGraph, OutputStream os)
            throws IOException {
        Output out = new Output(os);
        Kryo kryo = KRYOS.get();
        Registration reg = kryo.getRegistration(objectGraph.getClass());
        kryo.writeObject(out, objectGraph, reg.getSerializer());
        out.flush();
    }

    @Override
    public <T> T deserialize(InputStream is, Class<T> type) throws IOException {
        Kryo kryo = KRYOS.get();
        Registration reg = kryo.getRegistration(type);
        return type.cast(kryo.readObject(new Input(is), type,
                reg.getSerializer()));
    }

    @Override
    public Object deserialize(InputStream is) throws IOException,
            ClassNotFoundException {
        throw new UnsupportedOperationException();
    }
}
