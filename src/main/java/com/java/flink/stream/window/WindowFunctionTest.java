package com.java.flink.stream.window;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
