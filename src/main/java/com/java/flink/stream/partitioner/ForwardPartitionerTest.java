package com.java.flink.stream.partitioner;

import com.java.flink.stream.func.LocalTextFileUseMmapSourceFunctionV2;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardPartitionerTest {
    static final Logger LOG = LoggerFactory.getLogger(ForwardPartitionerTest.class);

    // 默认就是ForwardPartitioner，会在一个OperatorChain
    @Test
    public void testForward() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String filePath = "files/province_json.txt";
        DataStream<String> lines = env.addSource(new LocalTextFileUseMmapSourceFunctionV2(filePath, 2000, Long.MAX_VALUE, Integer.MAX_VALUE));

        DataStream<Tuple2<Integer, String>> oneDs = lines.map(new RichMapFunction<String, Tuple2<Integer, String>>() {
            int indexOfSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public Tuple2<Integer, String> map(String value) throws Exception {
                return Tuple2.of(indexOfSubtask, value);
            }
        });

        DataStream<Tuple2<Integer, String>> twoDs = oneDs.forward();

        twoDs.addSink(new RichSinkFunction<Tuple2<Integer, String>>() {
            int indexOfSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
                assert indexOfSubtask == value.f0;
                System.out.println(value.f0 + ", " + value.f1);
                //LOG.warn("out:" + value.f1);
            }
        });

        env.execute("ForwardPartitionerTest");
    }

    /**
     * disableChaining分成多个task，但是上游分区1还是对应下游分区1，数据是在本地进程通信的
     */
    @Test
    public void testForwardDisableChain() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(2);

        String filePath = "files/province_json.txt";
        DataStream<String> lines = env.addSource(new LocalTextFileUseMmapSourceFunctionV2(filePath, 2000, Long.MAX_VALUE, Integer.MAX_VALUE));

        DataStream<Tuple2<Integer, String>> oneDs = lines.map(new RichMapFunction<String, Tuple2<Integer, String>>() {
            int indexOfSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public Tuple2<Integer, String> map(String value) throws Exception {
                return Tuple2.of(indexOfSubtask, value);
            }
        });

        DataStream<Tuple2<Integer, String>> twoDs = oneDs.forward();


        twoDs.addSink(new RichSinkFunction<Tuple2<Integer, String>>() {
            int indexOfSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
                assert indexOfSubtask == value.f0;
                System.out.println(value.f0 + ", " + value.f1);
                //LOG.warn("out:" + value.f1);
            }
        }).disableChaining();

        env.execute("ForwardPartitionerTest");
    }
}
