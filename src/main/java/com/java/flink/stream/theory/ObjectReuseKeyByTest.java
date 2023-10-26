package com.java.flink.stream.theory;

import com.java.flink.stream.func.CustomSourceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

public class ObjectReuseKeyByTest {

    public static void main(String[] args) throws Exception {
        if(args[0].equals(1)){
            test1(args);
        } else if (args[0].equals(2)) {
            test2(args);
        }
    }

    public static void test1(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple4<Tuple2<Integer, Long>, Integer, Long, String>> ds = env.addSource(new CustomSourceFunction<Tuple4<Tuple2<Integer, Long>, Integer, Long, String>>() {
            Tuple4<Tuple2<Integer, Long>, Integer, Long, String> data = new Tuple4<>();
            Tuple2<Integer, Long> key = new Tuple2<>();
            long id = 1;

            @Override
            public Tuple4<Tuple2<Integer, Long>, Integer, Long, String> elementGene() {
                key.f0 = indexOfSubtask;
                key.f1 = id;
                data.f0 = key;
                data.f1 = indexOfSubtask;
                data.f2 = id;
                data.f3 = Long.toString(id);
                id++;
                return data;
            }
        });
        ds.keyBy(new KeySelector<Tuple4<Tuple2<Integer, Long>, Integer, Long, String>, Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> getKey(Tuple4<Tuple2<Integer, Long>, Integer, Long, String> value) throws Exception {
                return value.f0;
            }
        }).addSink(new RichSinkFunction<Tuple4<Tuple2<Integer, Long>, Integer, Long, String>>() {
            Map<Integer, Long> ids = new HashMap<>();
            int indexOfSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public void invoke(Tuple4<Tuple2<Integer, Long>, Integer, Long, String> data, Context context) throws Exception {
                Long id = ids.getOrDefault(data.f1, 0L);
                assert data.f2 > id;
                ids.put(data.f1, data.f2);

                System.out.println(indexOfSubtask + ":" + data);
            }
        });

        env.execute("ObjectReuseKeyByTest");
    }

    public static void test2(String[] args) throws Exception {
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
        ds.keyBy(new KeySelector<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>>() {
            Tuple2<Integer, Long> key = new Tuple2<>();
            @Override
            public Tuple2<Integer, Long> getKey(Tuple3<Integer, Long, String> data) throws Exception {
                key.f0 = data.f0;
                key.f1 = data.f1;
                return key;
            }
        }).addSink(new RichSinkFunction<Tuple3<Integer, Long, String>>() {
            Map<Integer, Long> ids = new HashMap<>();

            @Override
            public void invoke(Tuple3<Integer, Long, String> data, Context context) throws Exception {
                Long id = ids.getOrDefault(data.f0, 0L);
                assert data.f1 > id;
                ids.put(data.f0, data.f1);
                System.out.println(data);
            }
        });

        env.execute("ObjectReuseKeyByTest2");
    }
}
