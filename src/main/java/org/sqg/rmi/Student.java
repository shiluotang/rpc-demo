package org.sqg.rmi;

import java.io.Serializable;

public class Student implements Serializable {

    private static final long serialVersionUID = -7162122831439148570L;
    private String name;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(final int age) {
        this.age = age;
    }

}
