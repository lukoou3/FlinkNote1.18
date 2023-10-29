package com.java.flink.util;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class SingleValueMapTest {


    @Test
    public void releaseResourceData() throws Exception{
        Thread[] threads = new Thread[20];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                SingleValueMap.Data<ConnDada> connDada = null;
                try {
                    connDada = SingleValueMap.acquireData("conn_data", () -> new ConnDada(), x -> {
                        System.out.println("close conn");
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(5) * 10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                connDada.release();
            }, "Thread-" + i);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
    }

    @Test
    public void acquireNonResourceData(){

    }

    public static class ConnDada{
        public ConnDada(){
            System.out.println("ConnDada init");
        }
    }
}
