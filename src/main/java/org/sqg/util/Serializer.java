package org.sqg.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer {

	<T> void serialize(T objectGraph, OutputStream os) throws IOException;

	<T> byte[] serialize(T objectGraph);

	<T> T deserialize(InputStream is, Class<T> type) throws IOException;

	<T> T deserialize(byte[] bytes, Class<T> type);

	<T> T deserialize(byte[] bytes, int offset, int length, Class<T> type);

	Object deserialize(InputStream is) throws IOException, ClassNotFoundException;

	Object deserialize(byte[] bytes) throws ClassNotFoundException;

	Object deserialize(byte[] bytes, int offset, int length) throws ClassNotFoundException;
}
