package com.java.flink.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestSingleThreadScheduledExecutor {
    static final Logger LOG = LoggerFactory.getLogger("Test");

    public static void main(String[] args) throws Exception{
        /**
         * 之前一直以为ScheduledExecutorService是用于实现一个周期性的定时任务，其实不是，可以调度多个任务
         */
        ScheduledExecutorService scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("BatchInterval");
        scheduler.scheduleWithFixedDelay(() -> {
            LOG.warn("1000ms执行一次");
        }, 1000, 1000, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(() -> {
            LOG.warn("2000ms执行一次");
        }, 2000, 2000, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(() -> {
            LOG.warn("5000ms执行一次");
        }, 5000, 5000, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(() -> {
            LOG.warn("10000ms执行一次");
        }, 0, 10000, TimeUnit.MILLISECONDS);

        Thread.sleep(1000 * 60 * 3);
    }

}
