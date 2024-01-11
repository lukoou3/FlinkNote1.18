package com.java.flink.stream.window;

import com.java.flink.stream.func.LogMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class EventTimeWindowLateDataTest {

    /**
     * sideOutputLateData就是输出延时不能被分配到window中的元素，和allowedLateness设不设置没关系。延时分配vu到window中的元素都会作为延时元素输出到测流。
     * 输入 => 输出:
     *     1000 a
     *     2000 a
     *     9999 a
     *     10000 a => 触发窗口
     *               [1970-01-01 08:00:00,1970-01-01 08:00:10):a:(9999,a,3)
     *     9999 a =>
     *               延时严重的> (9999,a,1)
     *     1 a    =>
     *               延时严重的> (1,a,1)
     *     1 b     =>
     *               延时严重的> (1,b,1)
     *     20000 a =>  触发窗口
     *               [1970-01-01 08:00:10,1970-01-01 08:00:20):a:(10000,a,1)
     *     11000 a =>
     *               延时严重的> (11000,a,1)
     *
     */
    @Test
    public void testSideOutputLateData() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        /**
         * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
         * 在本地使用socket测试多并行度时，一定要设置WithWatermark的Stream为1，注意Watermark的触发条件
         * 生产条件就不必考虑，毕竟实时的数据还是来的比较快的，数据量不会小
         */
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        DataStream<Tuple3<Long, String, Integer>> ds = text.flatMap(sourceMapFunction()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f0)
        );

        final OutputTag<Tuple3<Long, String, Integer>> lateOutputTag = new OutputTag<Tuple3<Long, String, Integer>>("late-data"){};

        SingleOutputStreamOperator<String> rstDs = ds.map(new LogMap<>())
                .keyBy(x -> x.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateOutputTag)
                .reduce((a, b) -> {
                    return Tuple3.of(b.f0, b.f1, b.f2 + a.f2);
                }, windowFunction());
        // getSideOutput是SingleOutputStreamOperator的方法
        DataStream<Tuple3<Long, String, Integer>> lateDs = rstDs.getSideOutput(lateOutputTag);

        rstDs.print();

        lateDs.print("延时严重的");

        env.execute();
    }

    private RichFlatMapFunction<String, Tuple3<Long, String, Integer>> sourceMapFunction() {
        return new RichFlatMapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
                String[] words = value.trim().split("\\s+");
                if (words.length >= 2) {
                    try {
                        long ts = Long.parseLong(words[0]);
                        for (int i = 1; i < words.length; i++) {
                            out.collect(Tuple3.of(ts, words[i], 1));
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
        };
    }

    private ProcessWindowFunction<Tuple3<Long, String, Integer>, String, String, TimeWindow> windowFunction() {
        return new ProcessWindowFunction<Tuple3<Long, String, Integer>, String, String, TimeWindow>() {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public void process(String key, ProcessWindowFunction<Tuple3<Long, String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple3<Long, String, Integer>> elements, Collector<String> out) throws Exception {
                Tuple3<Long, String, Integer> acc = elements.iterator().next();
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                out.collect(String.format("[%s,%s):%s:%s", windowStart, windowEnd, key, acc.toString()));
            }
        };
    }
}
