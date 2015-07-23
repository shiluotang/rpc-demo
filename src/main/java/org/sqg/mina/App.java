package org.sqg.mina;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Student implements Serializable {

    private static final long serialVersionUID = 4392368223645816886L;
    private String name;
    private int age;

    public Student(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "{name = " + name + ", age = " + age + "}";
    }
}

public class App {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException {
        try (final Server server = new Server(1234) {
            @Override
            protected Object handleMessage(Object messageObj) {
                LOGGER.info("[SERVER]:[" + getLocalAddress() + "]:[RECEIVED]: "
                        + messageObj);
                return "OK";
            }
        }) {

            try (Client client = new Client(server.getLocalAddress()) {

                @Override
                protected Object handleMessage(final Object messageObj) {
                    LOGGER.info("[CLIENT]:[" + getLocalAddress()
                            + "]:[RECEIVED]: " + messageObj);
                    return null;
                }

            }) {
                client.send("FUCK YOU!!!");
                TimeUnit.SECONDS.sleep(1);
                client.send("FUCK YOU!!!");
                TimeUnit.SECONDS.sleep(1);
                client.send(new Student("SQG", 10000));
                TimeUnit.SECONDS.sleep(5);
            }
        }
    }
}
