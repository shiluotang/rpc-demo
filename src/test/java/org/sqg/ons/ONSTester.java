package org.sqg.ons;

import java.io.Serializable;

import org.apache.commons.lang.time.StopWatch;
import org.junit.Test;
import org.sqg.rpc.RpcContract;
import org.sqg.rpc.RpcProxy;
import org.sqg.rpc.RpcService;

public class ONSTester {

    @RpcContract
    public interface RpcGreetings extends RpcService {

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
    public void testOns() throws InterruptedException {
        final String TOPIC = "NM-YEKAI-TEST";
        try (org.sqg.ons.RpcServer server = new RpcServer(TOPIC,
                new Object[] { new RpcGreetingsImpl() })) {
            server.start();
            try (org.sqg.ons.RpcClient client = new org.sqg.ons.RpcClient(TOPIC)) {
                client.start();
                RpcGreetings greetings = RpcProxy.newProxyInstance(client,
                        RpcGreetings.class);
                Student s = new Student();
                s.setAge(1);
                s.setName("a");
                StopWatch watch = new StopWatch();
                watch.start();
                final int N = 100;
                for (int i = 0; i < N; ++i) {
                    System.out.println(greetings.hello(s));
                }
                watch.stop();
                System.out.println("QPS: " + (double)N / watch.getTime() * 1000);
            }
        }
    }
}
