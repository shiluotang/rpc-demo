package org.sqg.minatest;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Assert;
import org.junit.Test;

public class ClassLoaderTest {

    @Test
    public void testLoaderMultipleVersions() throws MalformedURLException,
            ClassNotFoundException {
        final String PROTOBUF_2_4_1 = "http://nexus.ycmsh.com/service/local/repositories/central/content/com/google/protobuf/protobuf-java/2.4.1/protobuf-java-2.4.1.jar";
        final String PROTOBUF_2_5_0 = "http://nexus.ycmsh.com/service/local/repositories/central/content/com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar";
        final String CLASS = "com.google.protobuf.InvalidProtocolBufferException";
        URLClassLoader loader1 = new URLClassLoader(new URL[] { new URL(
                PROTOBUF_2_4_1) });
        URLClassLoader loader2 = new URLClassLoader(new URL[] { new URL(
                PROTOBUF_2_5_0) });
        Class<?> clazz1 = Class.forName(CLASS, false, loader1);
        Class<?> clazz2 = Class.forName(CLASS, false, loader2);
        Class<?> clazz3 = null;
        try {
            clazz3 = Class.forName(CLASS, false, Thread.currentThread()
                    .getContextClassLoader());
        } catch (ClassNotFoundException e) {
        }

        Assert.assertNull(clazz3);
        Assert.assertEquals(CLASS, clazz1.getName());
        Assert.assertEquals(CLASS, clazz2.getName());
        Assert.assertNotEquals(clazz1, clazz2);
    }
}
