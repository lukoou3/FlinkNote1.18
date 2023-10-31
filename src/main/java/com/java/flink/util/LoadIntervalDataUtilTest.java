package com.java.flink.util;

import org.junit.Test;

public class LoadIntervalDataUtilTest {

    @Test
    public void test() throws Exception{
        LoadIntervalDataUtil<Long> util = LoadIntervalDataUtil.newInstance(() -> System.currentTimeMillis(),
                LoadIntervalDataOptions.builder().withIntervalMs(3000).build());
        System.out.println(util.data());

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            System.out.println(util.data());
        }

        util.stop();
    }
}
