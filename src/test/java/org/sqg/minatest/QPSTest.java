package org.sqg.minatest;

import java.io.Serializable;
import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.rpc.RpcContract;
import org.sqg.rpc.RpcProxy;
import org.sqg.thrift.ThriftClientBuilder;
import org.sqg.thrift.ThriftServiceContainerServer;
import org.sqg.thrift.generated.Greetings;

public class QPSTest {

    @RpcContract
    public interface RpcGreetings {

        String hello(Student s);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(QPSTest.class);

    private static final class Student implements Serializable {

        private static final long serialVersionUID = -4128754906652725339L;
        private String name;
        private int age;

        public Student() {
        }

        @SuppressWarnings("unused")
        public String getName() {
            return name;
        }

        @SuppressWarnings("unused")
        public int getAge() {
            return age;
        }

        public void setName(final String value) {
            name = value;
        }

        public void setAge(final int value) {
            age = value;
        }

        @Override
        public String toString() {
            return super.toString() + "{ name = " + name + ", age = " + age
                    + "}";
        }
    }

    @Test
    public void testSynchronizedMinaQPS() {
        try (final org.sqg.mina.RpcServer server = new org.sqg.mina.RpcServer(
                12345, new RpcGreetings() {
                    @Override
                    public String hello(Student s) {
                        return "OK";
                    }
                })) {
            server.start();
            try (org.sqg.mina.RpcClient client = new org.sqg.mina.RpcClient(
                    server.getLocalAddress())) {
                RpcGreetings greetings = RpcProxy.newProxyInstance(client,
                        RpcGreetings.class);
                Student s = new Student();
                s.setName("sqg");
                s.setAge(18);
                String response = null;
                final int N = 0x1fff;
                for (int i = 0; i < 10; ++i)
                    greetings.hello(s);
                long t1 = System.nanoTime();
                for (int i = 0; i < N; ++i)
                    Assert.assertEquals("OK", response = greetings.hello(s));
                long t2 = System.nanoTime();
                LOGGER.info("N = {}, total = {} ms, avg = {} ms, QPS = {}", N,
                        (t2 - t1) * 1e-6, (t2 - t1) * 1e-6 / N, N
                                / ((t2 - t1) * 1e-9));
                LOGGER.info("response is {}", response);
            }
        }
    }

    @Test
    public void testSynchronizedThriftQPS() throws TException {
        try (final ThriftServiceContainerServer server = new ThriftServiceContainerServer(
                12345, new Object[] { new Greetings.Iface() {
                    @Override
                    public String hello(org.sqg.thrift.generated.Student s) {
                        return "OK";
                    }
                } })) {
            server.start();
            Greetings.Client client = ThriftClientBuilder.blocking()
                    .remoteServer(new InetSocketAddress(12345))
                    .buildAndOpen(Greetings.Client.class);
            org.sqg.thrift.generated.Student s = new org.sqg.thrift.generated.Student();
            s.setName("sqg");
            s.setAge(18);
            String response = null;
            final int N = 0x1fff;
            for (int i = 0; i < 10; ++i)
                client.hello(s);
            long t1 = System.nanoTime();
            for (int i = 0; i < N; ++i)
                Assert.assertEquals("OK", response = client.hello(s));
            long t2 = System.nanoTime();
            LOGGER.info("N = {}, total = {} ms, avg = {} ms, QPS = {}", N,
                    (t2 - t1) * 1e-6, (t2 - t1) * 1e-6 / N, N
                            / ((t2 - t1) * 1e-9));
            LOGGER.info("response is {}", response);
            client.getInputProtocol().getTransport().close();
        }
    }

    @Test
    public void testSynchronizedNettyQPS() throws InterruptedException {
        try (org.sqg.netty.RpcServer server = new org.sqg.netty.RpcServer(
                12345, new RpcGreetings() {
                    @Override
                    public String hello(Student s) {
                        return "OK";
                    }
                })) {
            server.start();
            try (org.sqg.netty.RpcClient client = new org.sqg.netty.RpcClient(
                    server.getLocalAddress())) {
                RpcGreetings greetings = RpcProxy.newProxyInstance(client,
                        RpcGreetings.class);
                final int N = 0x1fff;
                String response = null;
                Student s = new Student();
                s.setName("sqg");
                s.setAge(18);
                for (int i = 0; i < 10; ++i)
                    response = greetings.hello(s);
                long t1 = System.nanoTime();
                for (int i = 0; i < N; ++i)
                    Assert.assertEquals("OK", response = greetings.hello(s));
                long t2 = System.nanoTime();
                LOGGER.info("N = {}, total = {} ms, avg = {} ms, QPS = {}", N,
                        (t2 - t1) * 1e-6, (t2 - t1) * 1e-6 / N, N
                                / ((t2 - t1) * 1e-9));
                LOGGER.info("response is {}", response);
            }
        }
    }

    @Test
    public void testSynchronizedRMIQPS() {
    }
}
