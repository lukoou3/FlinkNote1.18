package com.java.flink.stream.join;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * 这里使用union+keyBy+KeyedProcessFunction实现类似connect加上KeyedCoProcessFunction实现双流join
 * union+keyBy+KeyedProcessFunction应该更加通用，可以实现三流的join
 * 不知道官方的connect,KeyedCoProcessFunction是怎么实现的
 *
 * 这里实现的join每个key只保存最新的一个data，不会保存list，适合特定的业务
 * 实际官方实现的join每个key保存都是list。
 * 常用的一种需求是主流需要关联维度，主流每条都要输出，维度只包含最新的，维度来时不设置定时。之后可以实现这种功能的function。
 */
public class Union2JoinKeyedProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.getConfig().enableObjectReuse();

        // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
        DataStreamSource<String> text1 = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> text2 = env.socketTextStream("localhost", 9988);

        SingleOutputStreamOperator<Data1> stream1 = text1.flatMap(new FlatMapFunction<String, Data1>() {
            @Override
            public void flatMap(String value, Collector<Data1> out) throws Exception {
                String[] arrays = value.trim().split("\\s+");
                if (arrays.length >= 2) {
                    Data1 data1 = new Data1(Integer.parseInt(arrays[0]), arrays[1]);
                    out.collect(data1);
                    System.out.println(data1);
                }
            }
        });

        SingleOutputStreamOperator<Data2> stream2 = text2.flatMap(new FlatMapFunction<String, Data2>() {
            @Override
            public void flatMap(String value, Collector<Data2> out) throws Exception {
                String[] arrays = value.trim().split("\\s+");
                if (arrays.length >= 2) {
                    Data2 data2 = new Data2(Integer.parseInt(arrays[0]), Integer.parseInt(arrays[1]));
                    out.collect(data2);
                    System.out.println(data2);
                }
            }
        });

        Union2JoinKeyedProcessFunction.union(stream1, stream2, x->x.id, x->x.id, TypeInformation.of(new TypeHint<Union2JoinKeyedProcessFunction.Union2Data<Integer, Data1, Data2>>() {}) )
                .process(new Union2JoinKeyedProcessFunction<Integer, Data1, Data2>(3000, true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), TypeInformation.of(Data1.class), TypeInformation.of(Data2.class)))
                .map(new MapFunction<Union2JoinKeyedProcessFunction.Union2Data<Integer, Data1, Data2>, Data>() {
                    @Override
                    public Data map(Union2JoinKeyedProcessFunction.Union2Data<Integer, Data1, Data2> data) throws Exception {
                        int key = data.key;
                        Data1 data1 = data.data1;
                        Data2 data2 = data.data2;
                        if (data1 != null && data2 != null) {
                        } else if (data1 != null) {
                        } else if (data2 != null) {
                        }
                        return new Data(key, data1, data2);
                    }
                })
                .print();

        env.execute("Union2JoinKeyedProcessFunctionTest");
    }



    public static class Data1{
        public int id;
        public String name;

        public Data1(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Data1{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class Data2{
        public int id;
        public int age;

        public Data2(int id, int age) {
            this.id = id;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Data2{" +
                    "id=" + id +
                    ", age=" + age +
                    '}';
        }
    }

    public static class Data {
        public int id;
        public Data1 Data1;
        public Data2 Data2;
        public Data(int id,Data1 data1, Data2 data2) {
            this.id = id;
            Data1 = data1;
            Data2 = data2;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "id=" + id +
                    ", Data1=" + Data1 +
                    ", Data2=" + Data2 +
                    '}';
        }
    }
}
