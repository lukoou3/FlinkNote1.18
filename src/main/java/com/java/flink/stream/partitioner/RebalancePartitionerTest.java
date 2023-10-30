package com.java.flink.stream.partitioner;

import com.java.flink.stream.func.LocalTextFileUseMmapSourceFunctionV2;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalancePartitionerTest {
    static final Logger LOG = LoggerFactory.getLogger(RebalancePartitionerTest.class);

    /**
     * rebalance后，下游的流和前面的分开，这个容易理解，因为上游task的每个元素要向下游所有task发送
     * RecordWriterOutput的RecordWriter recordWriter 属性类型是ChannelSelectorRecordWriter， numberOfChannels就是下游并行度
     * ChannelSelectorRecordWriter的channelSelector类型是RebalancePartitioner
     */
    @Test
    public void testRebalance() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String filePath = "files/province_json.txt";
        DataStream<String> lines = env.addSource(new LocalTextFileUseMmapSourceFunctionV2(filePath, 2000, Long.MAX_VALUE, Integer.MAX_VALUE));

        DataStream<Tuple2<Integer, String>> oneDs = lines.flatMap(new RichFlatMapFunction<String, Tuple2<Integer, String>>() {
            int indexOfSubtask;
            int parallelism;
            int cnt = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
                parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, String>> out) throws Exception {
                if(cnt % parallelism == indexOfSubtask){
                    out.collect(Tuple2.of(indexOfSubtask, value));
                }
                cnt++;
            }
        });

        DataStream<Tuple2<Integer, String>> twoDs = oneDs.rebalance();

        twoDs.addSink(new RichSinkFunction<Tuple2<Integer, String>>() {
            int indexOfSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
                System.out.println(String.format("%d => %d: %s", value.f0 , indexOfSubtask,  value.f1));
                //LOG.warn("out:" + value.f1);
            }
        });

        env.execute("RebalancePartitionerTest");
    }


}
