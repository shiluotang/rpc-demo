package org.sqg.rmi;

public class RMIGreetingsImpl implements RMIGreetings {

    @Override
    public String hello(Student s) {
        return "OK";
    }

}
