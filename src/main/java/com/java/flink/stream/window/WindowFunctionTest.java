package com.java.flink.stream.window;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/operators/windows/#%E7%AA%97%E5%8F%A3%E5%87%BD%E6%95%B0window-functions
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/operators/windows/#window-functions
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/windows/#tumbling-windows
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/zh/docs/dev/datastream/operators/windows/#window-assigners
 *
 * 定义了 window assigner 之后，我们需要指定当窗口触发之后，我们如何计算每个窗口中的数据， 这就是 window function 的职责了。关于窗口如何触发，详见 triggers。
 *
 * 窗口函数有三种：ReduceFunction、AggregateFunction 或 ProcessWindowFunction。
 * 前两者执行起来更高效（详见 State Size）因为 Flink 可以在每条数据到达窗口后 进行增量聚合（incrementally aggregate）。
 * 而 ProcessWindowFunction 会得到能够遍历当前窗口内所有数据的 Iterable，以及关于这个窗口的 meta-information。
 *
 * 使用 ProcessWindowFunction 的窗口转换操作没有其他两种函数高效，因为 Flink 在窗口触发前必须缓存里面的所有数据。
 * ProcessWindowFunction 可以与 ReduceFunction 或 AggregateFunction 合并来提高效率。
 * 这样做既可以增量聚合窗口内的数据，又可以从 ProcessWindowFunction 接收窗口的 metadata。
 */
public class WindowFunctionTest {
    String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":0, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    /**
     * ProcessWindowFunction 有能获取包含窗口内所有元素的 Iterable， 以及用来获取时间和状态信息的 Context 对象，比其他窗口函数更加灵活。
     * ProcessWindowFunction 的灵活性是以性能和资源消耗为代价的， 因为窗口中的数据无法被增量聚合，而需要在窗口触发前缓存所有数据。
     */
    @Test
    public void testProcessWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ProcessWindowFunction processWindowFunction = new ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>.Context context, Iterable<OnlineLog> elements, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
                int visitCnt = 0;
                for (OnlineLog element : elements) {
                    visitCnt += element.visitCnt;
                }
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
            }
        };

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(processWindowFunction)
                .print();

        env.execute("WindowFunctionTest");
    }

    /**
     * ReduceFunction 指定两条输入数据如何合并起来产生一条输出数据，输入和输出数据的类型必须相同。 Flink 使用 ReduceFunction 对窗口中的数据进行增量聚合。
     * 输入和输出数据的类型必须相同：决定了这个方法用的较少
     */
    @Test
    public void testReduceFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((log1, log2) -> {log2.visitCnt += log1.visitCnt;return log2;})
                .print();

        env.execute("WindowFunctionTest");
    }

    /**
     * ReduceFunction 是 AggregateFunction 的特殊情况。
     * AggregateFunction 接收三个类型：输入数据的类型(IN)、累加器的类型（ACC）和输出数据的类型（OUT）。
     * 输入数据的类型是输入流的元素类型，AggregateFunction 接口有如下几个方法：创建初始累加器、把每一条元素加进累加器、合并两个累加器、从累加器中提取输出（OUT 类型）。
     *
     * 与 ReduceFunction 相同，Flink 会在输入数据到达窗口时直接进行增量聚合。
     */
    @Test
    public void testAggregateFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<OnlineLog, OnlineLog, String>() {
                    @Override
                    public OnlineLog createAccumulator() {
                        return new OnlineLog();
                    }

                    @Override
                    public OnlineLog add(OnlineLog value, OnlineLog accumulator) {
                        accumulator.pageId = value.pageId;
                        accumulator.visitCnt += value.visitCnt;
                        return accumulator;
                    }

                    @Override
                    public String getResult(OnlineLog acc) {
                        return acc.pageId + ":" + acc.visitCnt;
                    }

                    @Override
                    public OnlineLog merge(OnlineLog a, OnlineLog b) {
                        b.visitCnt += a.visitCnt;
                        return b;
                    }
                }).print();

        env.execute("WindowFunctionTest");
    }

    /**
     * AggregateFunction没法获取key和窗口时间，可以和ProcessWindowFunction结合
     */
    @Test
    public void testAggregateFunctionWithProcessWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<OnlineLog, OnlineLog, OnlineLog>() {
                    @Override
                    public OnlineLog createAccumulator() {
                        return new OnlineLog();
                    }

                    @Override
                    public OnlineLog add(OnlineLog value, OnlineLog accumulator) {
                        accumulator.pageId = value.pageId;
                        accumulator.visitCnt += value.visitCnt;
                        return accumulator;
                    }

                    @Override
                    public OnlineLog getResult(OnlineLog acc) {
                        return acc;
                    }

                    @Override
                    public OnlineLog merge(OnlineLog a, OnlineLog b) {
                        b.visitCnt += a.visitCnt;
                        return b;
                    }
                }, new ProcessWindowFunction<OnlineLog, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, ProcessWindowFunction<OnlineLog, String, Integer, TimeWindow>.Context context, Iterable<OnlineLog> elements, Collector<String> out) throws Exception {
                        OnlineLog acc = elements.iterator().next();
                        String windowStart = fmt.format(new Date(context.window().getStart()));
                        String windowEnd = fmt.format(new Date(context.window().getEnd()));
                        out.collect(String.format("%s:%d.%s,%s", key, acc.visitCnt, windowStart, windowEnd));
                    }
                }).print();

        env.execute("WindowFunctionTest");
    }

    /**
     * AggregateFunction没法获取key和窗口时间，可以和WindowFunction结合.和ProcessWindowFunction基本一样。
     *     在某些可以使用 ProcessWindowFunction 的地方，可以使用 WindowFunction。
     *     它是旧版的 ProcessWindowFunction，只能提供更少的环境信息且缺少一些高级的功能，比如 per-window state。
     *     这个接口会在未来被弃用。
     */
    @Test
    public void testAggregateFunctionWithWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<OnlineLog, OnlineLog, OnlineLog>() {
                    @Override
                    public OnlineLog createAccumulator() {
                        return new OnlineLog();
                    }

                    @Override
                    public OnlineLog add(OnlineLog value, OnlineLog accumulator) {
                        accumulator.pageId = value.pageId;
                        accumulator.visitCnt += value.visitCnt;
                        return accumulator;
                    }

                    @Override
                    public OnlineLog getResult(OnlineLog acc) {
                        return acc;
                    }

                    @Override
                    public OnlineLog merge(OnlineLog a, OnlineLog b) {
                        b.visitCnt += a.visitCnt;
                        return b;
                    }
                }, new WindowFunction<OnlineLog, String, Integer, TimeWindow>() {

                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<OnlineLog> input, Collector<String> out) throws Exception {
                        OnlineLog acc = input.iterator().next();
                        String windowStart = fmt.format(new Date(window.getStart()));
                        String windowEnd = fmt.format(new Date(window.getEnd()));
                        out.collect(String.format("%s:%d.%s,%s", key, acc.visitCnt, windowStart, windowEnd));
                    }
                }).print();

        env.execute("WindowFunctionTest");
    }

    public static class OnlineLog {
        public int pageId;
        public int userId;
        public long time;
        public int visitCnt;

        @Override
        public String toString() {
            return "OnlineLog{" +
                    "pageId=" + pageId +
                    ", userId=" + userId +
                    ", time=" + time +
                    ", visitCnt=" + visitCnt +
                    '}';
        }
    }
}
