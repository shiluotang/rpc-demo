package org.sqg.minatest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.util.JdkSerializer;
import org.sqg.util.KryoManualSerializer;
import org.sqg.util.KryoSerializer;
import org.sqg.util.Serializer;

public class SerializerTest {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SerializerTest.class);

    Map<Integer, Integer> data;

    @Before
    public void setUp() {
        data = new HashMap<>();
        data.put(1, 2);
        data.put(3, 4);
        data.put(5, 6);
    }

    @After
    public void tearDown() {
        if (data != null)
            data.clear();
        data = null;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testKryo() throws IOException, ClassNotFoundException {
        LOGGER.info("kryo");
        Serializer serializer = new KryoSerializer();
        Map<Integer, Integer> map = (Map<Integer, Integer>) serializer
                .deserialize(serializer.serialize(data), Map.class);
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            Assert.assertEquals(entry.getValue(), data.get(entry.getKey()));
            LOGGER.info("{}", entry);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testKryoManual() throws IOException, ClassNotFoundException {
        LOGGER.info("kryo-manual");
        Serializer serializer = new KryoManualSerializer();
        Map<Integer, Integer> map = (Map<Integer, Integer>) serializer
                .deserialize(serializer.serialize(data), data.getClass());
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            Assert.assertEquals(entry.getValue(), data.get(entry.getKey()));
            LOGGER.info("{}", entry);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testJdk() throws IOException {
        LOGGER.info("jdk");
        Serializer serializer = new JdkSerializer();
        Map<Integer, Integer> map = (Map<Integer, Integer>) serializer
                .deserialize(serializer.serialize(data), Map.class);
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            Assert.assertEquals(entry.getValue(), data.get(entry.getKey()));
            LOGGER.info("{}", entry);
        }
    }
}
