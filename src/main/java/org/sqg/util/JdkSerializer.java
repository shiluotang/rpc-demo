package org.sqg.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.sqg.mina.AbstractSerializer;

public final class JdkSerializer extends AbstractSerializer {

    @Override
    public <T> void serialize(T objectGraph, OutputStream os)
            throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(objectGraph);
    }

    @Override
    public <T> T deserialize(InputStream is, Class<T> type) throws IOException {
        try {
            return type.cast(deserialize(is));
        } catch (ClassNotFoundException e) {
        }
        return null;
    }

    @Override
    public Object deserialize(InputStream is) throws IOException,
            ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(is);
        return ois.readObject();
    }
}
