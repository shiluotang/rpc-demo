package org.sqg.rmi;

import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMIServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RMIServer.class);

    private Registry registry;
    private RMIGreetings greetings;

    public RMIServer() {
        System.setProperty("java.rmi.server.ignoreStubClasses", "true");
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new SecurityManager());
        this.greetings = new RMIGreetingsImpl();
    }

    public void start() throws RemoteException {
        if (registry == null) {
            synchronized (this) {
                if (registry == null) {
                    LOGGER.info("server starting at {}.",
                            new InetSocketAddress(Registry.REGISTRY_PORT));
                    AccessController.doPrivileged(new PrivilegedAction<Void>() {
                        @Override
                        public Void run() {
                            RMIGreetings stub;
                            Registry r = null;
                            try {
                                stub = ((RMIGreetings) UnicastRemoteObject
                                        .exportObject(greetings, 0));
                                r = LocateRegistry
                                        .getRegistry(Registry.REGISTRY_PORT);
                                if (r == null) {
                                    LOGGER.info("try to create registry on {}",
                                            Registry.REGISTRY_PORT);
                                    r = LocateRegistry
                                            .createRegistry(Registry.REGISTRY_PORT);
                                }
                                LOGGER.info("registry is {}", r);
                                r.rebind(RMIGreetings.class.getName(), stub);
                            } catch (RemoteException e) {
                                throw new RuntimeException(e);
                            }
                            registry = r;
                            return null;
                        }
                    });
                    LOGGER.info("server started at {}.", new InetSocketAddress(
                            Registry.REGISTRY_PORT));
                }
            }
        }
    }

    public void stop() {
        if (registry != null) {
            synchronized (this) {
                Registry r = registry;
                if (r != null) {
                    LOGGER.info("stopping server at {}", new InetSocketAddress(
                            Registry.REGISTRY_PORT));
                    try {
                        r.unbind(RMIGreetings.class.getName());
                    } catch (RemoteException | NotBoundException e) {
                        throw new RuntimeException(e);
                    }
                    registry = null;
                    LOGGER.info("stopped server at {}", new InetSocketAddress(
                            Registry.REGISTRY_PORT));
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
    }
}