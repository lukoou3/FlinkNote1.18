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

    /**
     * 基于事件滚动窗口：每10秒计算个页面的pv
     * 默认情况下，当watermark通过end-of-window激活window计算结束之后，再有之前的数据到达时，这些数据会被删除。
     * 为了避免有些迟到的数据被删除，可以设置sideOutputLateData，把迟到严重的数据输出到侧输出流中
     * 还有一个配置是allowedLateness，允许窗口延时销毁，
     *  设置allowedLateness(Time.seconds(2))：
     *      就代表[0, 10)的窗口在Watermark到达10秒触发，但是窗口在Watermark到达12秒才会完全销毁，在此期间有[0, 10)的元素到来，会触发窗口计算
     *      在Watermark到达12秒后，再有[0, 10)的元素到来，会被任务迟到数据，被发送到sideOutputLateData或者忽略删除
     * sideOutputLateData和allowedLateness是两个单独的配置，没有依赖关系。
     * 尽量不要设置allowedLateness，默认会重复触发窗口，sql似乎就没有这个配置。
     *
     * 测试示例，输入 => 输出:
     *     1000 a
     *     2000 a
     *     9999 a
     *     10000 a => 触发窗口
     *               [1970-01-01 08:00:00,1970-01-01 08:00:10):a:(9999,a,3)
     *     9999 a => 再次触发窗口
     *               [1970-01-01 08:00:00,1970-01-01 08:00:10):a:(9999,a,4)
     *     9999 b => 再次触发窗口(仅仅计算这个key，而不是全部的key)
     *               [1970-01-01 08:00:00,1970-01-01 08:00:10):b:(9999,b,1)
     *     9999 a => 再次触发窗口(仅仅计算这个key，而不是全部的key)
     *               [1970-01-01 08:00:00,1970-01-01 08:00:10):a:(9999,a,5)
     *     11000 a
     *     1 a    => 再次触发窗口
     *               [1970-01-01 08:00:00,1970-01-01 08:00:10):a:(1,a,6)
     *     11999 a
     *     1 a    => 再次触发窗口
     *               [1970-01-01 08:00:00,1970-01-01 08:00:10):a:(1,a,7)
     *     12000 a ([0, 10)区间的窗口被彻底销毁，因为Watermark已到达end-of-window + allowedLateness)
     *     1 a    =>
     *               延时严重的> (1,a,1)
     *     9999 a    =>
     *               延时严重的> (1,9999,1)
     *
     *
     *
     * 官网关于窗口的生命周期的描述：
     *    简单来说，一个窗口在第一个属于它的元素到达时就会被创建，然后在时间（event 或 processing time） 超过窗口的“结束时间戳 + 用户定义的 allowed lateness （详见 Allowed Lateness）”时 被完全删除。
     *    Flink 仅保证删除基于时间的窗口，其他类型的窗口不做保证， 比如全局窗口（详见 Window Assigners）。
     *    例如，对于一个基于 event time 且范围互不重合（滚动）的窗口策略， 如果窗口设置的时长为五分钟、可容忍的迟到时间（allowed lateness）为 1 分钟，
     *    那么第一个元素落入 12:00 至 12:05 这个区间时，Flink 就会为这个区间创建一个新的窗口。当 watermark 越过 12:06 时，这个窗口将被摧毁。
     *
     * 官网关于迟到数据的一些考虑的描述：
     *    当指定了大于 0 的 allowed lateness 时，窗口本身以及其中的内容仍会在 watermark 越过窗口末端后保留。
     *    这时，如果一个迟到但未被丢弃的数据到达，它可能会再次触发这个窗口。 这种触发被称作 late firing，与表示第一次触发窗口的 main firing 相区别。
     *    如果是使用会话窗口的情况，late firing 可能会进一步合并已有的窗口，因为他们可能会连接现有的、未被合并的窗口。
     *
     *    你应该注意：late firing 发出的元素应该被视作对之前计算结果的更新，即你的数据流中会包含一个相同计算任务的多个结果。你的应用需要考虑到这些重复的结果，或去除重复的部分。
     */
    @Test
    public void testAllowedLateness() throws Exception {
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
                .allowedLateness(Time.seconds(2))
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
