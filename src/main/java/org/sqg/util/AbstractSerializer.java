package org.sqg.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class AbstractSerializer implements Serializer {

	@Override
	public final <T> byte[] serialize(final T objectGraph) {
		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			serialize(objectGraph, os);
			return os.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final <T> T deserialize(final byte[] bytes, int offset, int length, final Class<T> type) {
		try (ByteArrayInputStream is = new ByteArrayInputStream(bytes, offset, length)) {
			return deserialize(is, type);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final <T> T deserialize(final byte[] bytes, final Class<T> type) {
		try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
			return deserialize(is, type);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final Object deserialize(final byte[] bytes) throws ClassNotFoundException {
		try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
			return deserialize(is);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final Object deserialize(final byte[] bytes, int offset, int length) throws ClassNotFoundException {
		try (ByteArrayInputStream is = new ByteArrayInputStream(bytes, offset, length)) {
			return deserialize(is);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
