package com.java.flink.stream.base;

import com.java.flink.stream.func.FieldGeneSouce;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FieldGeneSouceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String fieldGenesDesc = "[{\"type\":\"int_random\", \"fields\":{\"name\":\"id\"}}, {\"type\":\"str_fix\", \"fields\":{\"name\":\"name\",\"len\":5}}]";
        DataStream<String> ds = env.addSource(new FieldGeneSouce(fieldGenesDesc));

        ds.print();

        env.execute("FieldGeneSouceTest");
    }

}
