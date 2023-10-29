package com.java.flink.stream.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 之前的SourceFunction已经标记废弃，因为Checkpoint锁、复杂的线程模型等问题。
 * 看看NumberSequenceSource怎么实现新的Source
 * 新版本并没有废弃SinkFunction，估计因为SinkFunction没有SourceFunction的线程问题吧，它使用的就是actor/mailbox无锁线程模型。
 */
public class NumberSequenceSourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Long> ds = env.fromSource(new NumberSequenceSource(0, Long.MAX_VALUE), WatermarkStrategy.noWatermarks(), "Sequence Source");
        DataStream<Tuple2<Integer, Long>> rstDs = ds.map(new RichMapFunction<Long, Tuple2<Integer, Long>>() {
            int indexOfSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public Tuple2<Integer, Long> map(Long value) throws Exception {
                return Tuple2.of(indexOfSubtask, value);
            }
        });
        rstDs.addSink(new RichSinkFunction<Tuple2<Integer, Long>>() {
            @Override
            public void invoke(Tuple2<Integer, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("NumberSequenceSourceTest");
    }

}
