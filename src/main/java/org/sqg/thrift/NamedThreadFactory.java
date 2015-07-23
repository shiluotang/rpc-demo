package org.sqg.thrift;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * All threads created by this object will have the same prefix.
 *
 * @author apache.org
 * @author <a href="mailto:quangang.sheng@adchina.com">Quangang Sheng</a>
 * @version 2015/7/6 19:28
 * @since 1.0
 */
public final class NamedThreadFactory implements ThreadFactory {

    /**
     * Index number for different pool and different prefix.
     */
    private static final ConcurrentMap<String, AtomicInteger> POOL_NUMBER_MAPPING = new ConcurrentHashMap<String, AtomicInteger>();

    /**
     * The thread group that current thread belongs to.
     */
    private final ThreadGroup group;

    /**
     * Generated thread index sequencer.
     */
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    /**
     * The name prefix for every thread created by this {@link ThreadFactory}.
     */
    private final String namePrefix;

    /**
     * Construct an instance of {@link NamedThreadFactory}.
     *
     * @param name The name prefix.
     */
    public NamedThreadFactory(final String name) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
                .getThreadGroup();
        POOL_NUMBER_MAPPING.putIfAbsent(name, new AtomicInteger(0));
        AtomicInteger number = POOL_NUMBER_MAPPING.get(name);
        namePrefix = name + "-" + number.incrementAndGet() + "-thread-";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Thread newThread(final Runnable r) {
        Thread t = new Thread(group, r, namePrefix
                + threadNumber.getAndIncrement(), 0);
        if (t.isDaemon())
            t.setDaemon(false);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}
