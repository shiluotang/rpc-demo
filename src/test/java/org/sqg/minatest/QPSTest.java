package org.sqg.minatest;

import java.io.Serializable;
import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.mina.BlockingClient;
import org.sqg.mina.Server;
import org.sqg.netty.RpcServer;
import org.sqg.thrift.ThriftClientBuilder;
import org.sqg.thrift.ThriftServiceContainerServer;
import org.sqg.thrift.generated.Greetings;

public class QPSTest {

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
        try (final Server server = new Server(12345) {

            @Override
            protected Object handleMessage(Object messageObj) {
                // LOGGER.info("RECEIVED = {}", messageObj);
                return "OK";
            }
        }) {
            try (final BlockingClient client = new BlockingClient(
                    server.getLocalAddress())) {
                Student s = new Student();
                s.setName("sqg");
                s.setAge(18);
                Object response = null;
                final int N = 0xffff;
                long t1 = System.nanoTime();
                for (int i = 0; i < N; ++i) {
                    response = client.request(s);
                }
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
            Object response = null;
            final int N = 0xffff;
            long t1 = System.nanoTime();
            for (int i = 0; i < N; ++i)
                response = client.hello(s);
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
        try (RpcServer server = new RpcServer(12345)) {
            server.start();
        }
    }

    @Test
    public void testSynchronizedRMIQPS() {
    }
}
