package com.java.flink.stream.proto.convert.formats;

import org.junit.Test;

public class PerformanceTest {

    @Test
    public void testForAssign1(){
        byte[] fields = new byte[200];

        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 1000000; i++) {
                for (int j = 0; j < fields.length; j++) {
                    fields[j] = 0;
                }
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }
    }

    @Test
    public void testForAssign2(){
        byte[] fields = new byte[20];
        byte[] fieldsEmpty = new byte[20];

        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 1000000; i++) {
                System.arraycopy(fieldsEmpty, 0, fields, 0, fields.length);
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }
    }

    @Test
    public void testForAssign3(){
        FieldFlag[] fields = new FieldFlag[200];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new FieldFlag();
        }

        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 100000; i++) {
                for (int j = 0; j < fields.length; j++) {
                    fields[j].isNull = true;
                }
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }
    }

    public static class FieldFlag{
        boolean isNull = true;
        private double value;
    }
}
