package org.sqg.rmi;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIClient implements RMIGreetings, AutoCloseable {

    private RMIGreetings proxy;

    public RMIClient() throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry();
        proxy = (RMIGreetings) registry.lookup(RMIGreetings.class.getName());
    }

    @Override
    public String hello(final Student s) throws RemoteException {
        return proxy.hello(s);
    }

    @Override
    public void close() {
        proxy = null;
    }
}
