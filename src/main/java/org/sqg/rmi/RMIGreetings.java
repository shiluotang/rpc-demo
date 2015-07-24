package org.sqg.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RMIGreetings extends Remote {

    String hello(Student s) throws RemoteException;
}
