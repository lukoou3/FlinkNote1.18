package com.java.flink.util;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

public class ThreadUtils {

    /**
     * 包含单个线程的 ScheduledThreadPoolExecutor
     * Wrapper over ScheduledThreadPoolExecutor.
     */
    public static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String threadName) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }
}
