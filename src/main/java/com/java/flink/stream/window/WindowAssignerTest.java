package com.java.flink.stream.window;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/windows/#window-assigners
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/zh/docs/dev/datastream/operators/windows/#window-assigners
 */
public class WindowAssignerTest {

    String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":0, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    @Test
    public void testTumblingProcessingTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，5秒10个
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((log1, log2) -> {
                    log2.visitCnt += log1.visitCnt;
                    return log2;
                }, windowFunction())
                .print();

        env.execute();
    }

    // 元素分配窗口，竟然不是复制元素到每个窗口，而是直接发给每个窗口
    @Test
    public void testSlidingProcessingTimeWindowsWrong() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，5秒10个，10秒20个
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .reduce((log1, log2) -> {
                    // 元素分配窗口，竟然不是复制元素到每个窗口，而是直接发给每个窗口
                    // 使用agg函数就没这个问题，每个聚合对象是需要创建初始化的
                    log2.visitCnt += log1.visitCnt;
                    return log2;
                }, windowFunction())
                .print();

        env.execute();
    }

    @Test
    public void testSlidingProcessingTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，5秒10个，10秒20个
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .reduce((log1, log2) -> {
                    // 元素分配窗口，竟然不是复制元素到每个窗口，而是直接发给每个窗口
                    OnlineLog log = log2.copy();
                    log.visitCnt += log1.visitCnt;
                    return log;
                }, windowFunction())
                .print();

        env.execute();
    }

    /**
     * SlidingProcessingTimeWindows要慎用，看下WindowOperator就知道对于像每2秒计算前10分钟窗口的数据，每个数据会被划分到5个窗口中，保存在state中。
     * 分配窗口逻辑调用的WindowAssigner抽象类的assignWindows方法。
     * TumblingProcessingTimeWindows.assignWindows方法返回1个元素的List<TimeWindow>
     * SlidingProcessingTimeWindows.assignWindows方法基本返回size/slide大小的List<TimeWindow>
     * 要是聚合类的算子还好说，要是需要保存明细的，那对内存来说是个大的灾难
     *
     * @org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.processElement 官网关于状态大小的考量：https://nightlies.apache.org/flink/flink-docs-release-1.18/zh/docs/dev/datastream/operators/windows/#%E5%85%B3%E4%BA%8E%E7%8A%B6%E6%80%81%E5%A4%A7%E5%B0%8F%E7%9A%84%E8%80%83%E9%87%8F
     */
    @Test
    public void testSlidingProcessingTimeWindowsState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，5秒10个，10秒20个
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .reduce((log1, log2) -> {
                    // 元素分配窗口，竟然不是复制元素到每个窗口，而是直接发给每个窗口
                    OnlineLog log = log2.copy();
                    log.visitCnt += log1.visitCnt;
                    return log;
                }, windowFunction())
                .print();

        env.execute();
    }

    @Test
    public void testWatermarkOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，5秒10个，实际过去5秒
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000)).setParallelism(1);

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).setParallelism(1).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        // 事件时间窗口是左闭右开区间
        dsWithWatermark.map(x -> x).print();

        env.execute();
    }

    @Test
    public void testWatermark() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，5秒10个，实际过去5秒
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        // 事件时间窗口是左闭右开区间
        dsWithWatermark.map(x -> x).print().setParallelism(1);

        env.execute();
    }

    @Test
    public void testTumblingEventTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，5秒10个，实际过去5秒
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        // 事件时间窗口是左闭右开区间
        dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((log1, log2) -> {
                    log2.visitCnt += log1.visitCnt;
                    return log2;
                }, windowFunction())
                .print();

        env.execute();
    }

    @Test
    public void testSlidingEventTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每秒2个，10秒20个，实际过去10秒
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        // 事件时间窗口是左闭右开区间
        dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .reduce((log1, log2) -> {
                    // 元素分配窗口，竟然不是复制元素到每个窗口，而是直接发给每个窗口
                    OnlineLog log = log2.copy();
                    log.visitCnt += log1.visitCnt;
                    return log;
                }, windowFunction())
                .print();

        env.execute();
    }

    private ProcessWindowFunction<OnlineLog, String, Integer, TimeWindow> windowFunction() {
        return new ProcessWindowFunction<OnlineLog, String, Integer, TimeWindow>() {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public void process(Integer key, ProcessWindowFunction<OnlineLog, String, Integer, TimeWindow>.Context context, Iterable<OnlineLog> elements, Collector<String> out) throws Exception {
                OnlineLog acc = elements.iterator().next();
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                out.collect(String.format("[%s,%s):%s:%s", windowStart, windowEnd, key, acc.toString()));
            }
        };
    }

    public static class OnlineLog {
        public int pageId;
        public int userId;
        public long time;
        public int visitCnt;

        public OnlineLog copy() {
            OnlineLog copy = new OnlineLog();
            copy.pageId = pageId;
            copy.userId = userId;
            copy.time = time;
            copy.visitCnt = visitCnt;
            return copy;
        }

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
