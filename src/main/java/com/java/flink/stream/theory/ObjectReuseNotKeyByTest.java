package com.java.flink.stream.theory;

import com.java.flink.stream.func.CustomSourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * 多个task，没有key by的shuffle，全局使用一个对象是没问题的，task每输出一个元素立马序列化到buffer，数据不会错乱
 */
public class ObjectReuseNotKeyByTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple3<Integer, Long, String>> ds = env.addSource(new CustomSourceFunction<Tuple3<Integer, Long, String>>() {
            Tuple3<Integer, Long, String> data = new Tuple3<>();
            long id = 1;

            @Override
            public Tuple3<Integer, Long, String> elementGene() {
                data.f0 = indexOfSubtask;
                data.f1 = id;
                data.f2 = Long.toString(id);
                id++;
                return data;
            }
        });

        ds.rebalance().addSink(new RichSinkFunction<Tuple3<Integer, Long, String>>() {
            Map<Integer, Long> ids = new HashMap<>();

            @Override
            public void invoke(Tuple3<Integer, Long, String> data, Context context) throws Exception {
                Long id = ids.getOrDefault(data.f0, 0L);
                assert data.f1 > id;
                ids.put(data.f0, data.f1);
                System.out.println(data);
            }
        });

        env.execute("ObjectReuseNotKeyByTest");
    }

}
