package org.sqg.minatest;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqg.ons.RpcServer;
import org.sqg.rpc.RpcClient;
import org.sqg.rpc.RpcContract;
import org.sqg.rpc.RpcProxy;
import org.sqg.rpc.RpcRequest;
import org.sqg.rpc.RpcResponse;
import org.sqg.thrift.ThriftClientBuilder;
import org.sqg.thrift.ThriftServiceContainerServer;
import org.sqg.thrift.generated.Greetings;

public class QPSTest {

    private static final int N = 0x1ffff;

    public static final class DirectRpcClient extends org.sqg.rpc.RpcClient {

        private Object target;

        public DirectRpcClient(final Object target) {
            this.target = Objects.requireNonNull(target);
        }

        @Override
        public void close() {
            this.target = null;
        }

        @Override
        protected RpcResponse doRPC(RpcRequest request) {
            RpcResponse response = new RpcResponse();
            try {
                response.setResult(request.getMethod().invoke(target,
                        request.getArguments()));
            } catch (Exception e) {
                response.setThrowable(e);
            }
            return response;
        }
    }

    @RpcContract
    public interface RpcGreetings {

        String hello(Student s);

        void hello2();
    }

    private static final class RpcGreetingsImpl implements RpcGreetings {
        @Override
        public String hello(Student s) {
            return "OK";
        }

        @Override
        public void hello2() {
        }
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
                12345, new RpcGreetingsImpl())) {
            server.start();
            try (org.sqg.mina.RpcClient client = new org.sqg.mina.RpcClient(
                    server.getLocalAddress())) {
                RpcGreetings greetings = RpcProxy.newProxyInstance(client,
                        RpcGreetings.class);
                Student s = new Student();
                s.setName("sqg");
                s.setAge(18);
                long t1 = System.nanoTime();
                for (int i = 0; i < N; ++i)
                    greetings.hello(s);
                long t2 = System.nanoTime();
                LOGGER.info("{}:\t\tN = {},\tQPS = {}", Thread.currentThread()
                        .getStackTrace()[1].getMethodName(), N, N
                        / ((t2 - t1) * 1e-9));
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
            long t1 = System.nanoTime();
            for (int i = 0; i < N; ++i)
                client.hello(s);
            long t2 = System.nanoTime();
            LOGGER.info("{}:\tN = {},\tQPS = {}", Thread.currentThread()
                    .getStackTrace()[1].getMethodName(), N, N
                    / ((t2 - t1) * 1e-9));
            client.getInputProtocol().getTransport().close();
        }
    }

    @Test
    public void testSynchronizedNettyQPS() throws InterruptedException {
        try (org.sqg.netty.RpcServer server = new org.sqg.netty.RpcServer(
                12345, new RpcGreetingsImpl())) {
            server.start();
            try (org.sqg.netty.RpcClient client = new org.sqg.netty.RpcClient(
                    server.getLocalAddress())) {
                RpcGreetings greetings = RpcProxy.newProxyInstance(client,
                        RpcGreetings.class);
                Student s = new Student();
                s.setName("sqg");
                s.setAge(18);
                long t1 = System.nanoTime();
                for (int i = 0; i < N; ++i)
                    greetings.hello(s);
                long t2 = System.nanoTime();
                LOGGER.info("{}:\tN = {},\tQPS = {}", Thread.currentThread()
                        .getStackTrace()[1].getMethodName(), N, N
                        / ((t2 - t1) * 1e-9));
            }
        }
    }

    @Test
    public void testNoNetworkRPCQPS() {
        try (RpcClient client = new DirectRpcClient(new RpcGreetingsImpl())) {
            RpcGreetings greetings = RpcProxy.newProxyInstance(client,
                    RpcGreetings.class);
            Student s = new Student();
            s.setName("sqg");
            s.setAge(18);
            long t1 = System.nanoTime();
            for (int i = 0; i < N; ++i)
                greetings.hello(s);
            long t2 = System.nanoTime();
            LOGGER.info("{}:\t\tN = {},\tQPS = {}", Thread.currentThread()
                    .getStackTrace()[1].getMethodName(), N, N
                    / ((t2 - t1) * 1e-9));
        }
    }

    @Test
    public void testDirectObjectCallQPS() {
        RpcGreetings greetings = new RpcGreetingsImpl();
        Student s = new Student();
        s.setName("sqg");
        s.setAge(18);
        long t1 = System.nanoTime();
        for (int i = 0; i < N; ++i)
            greetings.hello(s);
        long t2 = System.nanoTime();
        LOGGER.info("{}:\t\tN = {},\tQPS = {}", Thread.currentThread()
                .getStackTrace()[1].getMethodName(), N, N / ((t2 - t1) * 1e-9));
    }

    @Test
    public void testBlockingQueueQPS() throws InterruptedException {
        BlockingQueue<Integer> responses = new ArrayBlockingQueue<>(1);
        long t1 = System.nanoTime();
        for (int i = 0; i < N; ++i) {
            responses.put(i);
            responses.take();
        }
        long t2 = System.nanoTime();
        LOGGER.info("{}:\t\tN = {},\tQPS = {}", Thread.currentThread()
                .getStackTrace()[1].getMethodName(), N, N / ((t2 - t1) * 1e-9));
    }

    @Test
    public void testOns() throws InterruptedException {
        final String TOPIC = "NM-YEKAI-TEST";
        try (org.sqg.ons.RpcServer server = new RpcServer(TOPIC,
                new Object[] { new RpcGreetingsImpl() })) {
            server.start();
            try (org.sqg.ons.RpcClient client = new org.sqg.ons.RpcClient(TOPIC)) {
                client.start();
                RpcGreetings greetings = RpcProxy.newProxyInstance(client,
                        RpcGreetings.class);
                System.out.println(greetings.hello(new Student()));
            }
        }
    }
}
