package org.sqg.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.sqg.mina.AbstractSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class KryoSerializer extends AbstractSerializer {

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
    public <T> void serialize(final T objectGraph, final OutputStream os)
            throws IOException {
        Output out = new Output(os);
        KRYOS.get().writeClassAndObject(out, objectGraph);
        out.flush();
    }

    @Override
    public <T> T deserialize(final InputStream is, final Class<T> type)
            throws IOException {
        try {
            return type.cast(deserialize(is));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object deserialize(InputStream is) throws IOException,
            ClassNotFoundException {
        return KRYOS.get().readClassAndObject(new Input(is));
    }
}
