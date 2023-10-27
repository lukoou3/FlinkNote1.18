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

/**
 * KeySelector产生的key，不会直接把key写入下游，上游根据key的hash发生到下游的Channel，下游根据KeySelector.getKey(value)重新获取key
 * KeySelector.getKey方法有两个调用的地方一是上游计算发生那个Channel，二是下游根据value获取key
 * 所以KeySelector.getKey对每个value生成的值必须是固定的，确定说必须是hash固定的，否则可能出现错误，所以flink内部校验了每个task hash后的范围，不在范围会直接抛出异常
 *
 * 上游调用的地方：
 * @org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner#selectChannel
 *
 * 下游调用的地方：
 * @org.apache.flink.streaming.api.operators.AbstractStreamOperator#setKeyContextElement
 *
 * 根据这个方法计算每个task key hash处理后的keyGroupRange，maxParallelism默认是128，2个并行度则:task-0 keyGroupRange:[0, 63], task-1 keyGroupRange:[64, 127]
 * org.apache.flink.runtime.state.KeyGroupRangeAssignment#computeKeyGroupRangeForOperatorIndex(int maxParallelism, int parallelism, int operatorIndex)
 *
 * 下游setCurrentKey、setCurrentKeyGroupIndex时!keyGroupRange.contains(currentKeyGroupIndex)时会抛出异常：
 *   Key group 94 is not in KeyGroupRange{startKeyGroup=0, endKeyGroup=63}. Unless you're directly using low level state access APIs, this is most likely caused by non-deterministic shuffle key (hashCode and equals implementation).
 *
 * 所以KeySelector的key理论上是可以复用的，但必须保证hash一致性，而且下游任务处理时需要注意key是否缓存问题，
 * 最好不要复用key，比如flink内部使用状态key是否复制，使用定时器时是否复制key
 */
public class ObjectReuseKeyByTest {

    public static void main(String[] args) throws Exception {
        if(args[0].equals("1")){
            testNormal1(args);
        } else if (args[0].equals("2")) {
            testNormal2(args);
        } else if (args[0].equals("3")) {
            testBad(args);
        }
    }

    public static void testNormal1(String[] args) throws Exception {
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

    public static void testNormal2(String[] args) throws Exception {
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

    public static void testBad(String[] args) throws Exception {
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
            long rand = 0;
            Tuple2<Integer, Long> key = new Tuple2<>();
            @Override
            public Tuple2<Integer, Long> getKey(Tuple3<Integer, Long, String> data) throws Exception {
                key.f0 = data.f0;
                key.f1 = rand;
                rand ++;
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
