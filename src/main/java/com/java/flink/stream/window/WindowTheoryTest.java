package com.java.flink.stream.window;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class WindowTheoryTest {
    String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":0, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    /**
     * 使用TumblingEventTimeWindows + ProcessWindowFunction
     * 核心实现逻辑在org.apache.flink.streaming.runtime.operators.windowing.WindowOperator：
     *    windowStateDescriptor属性是ListStateDescriptor，生成的windowState属性是HeapListState
     *    核心处理数据方法是processElement、onEventTime、onProcessingTime、emitWindowContents，和自己定义keyedProcessFunction实现一样
     *    这些状态操作都是基于key的
     *    processElement(StreamRecord<IN> element)方法：
     *        通过windowAssigner(TumblingEventTimeWindows)分配window
     *        windowState.setCurrentNamespace(window)， windowState.add(element.getValue())
     *        通过trigger(EventTimeTrigger)直接触发窗口计算和注册定时器触发窗口计算。
     *        如果trigger返回isFire直接触发窗口计算，返回isPurge清空windowState
     *        注册窗口销毁定时：registerCleanupTimer(window)。销毁时间，事件时间是window.maxTimestamp() + allowedLateness，处理时间是window.maxTimestamp()。
     *    onEventTime(InternalTimer<K, W> timer)方法：
     *        调用trigger.onEventTime返回triggerResult
     *        triggerResult如果isFire直接触发窗口计算，如果isPurge清空windowState
     *        如果isCleanupTime(window, timer.getTimestamp())，清空windowState，销毁窗口
     *    onProcessingTime(InternalTimer<K, W> timer)方法：
     *        和onEventTime逻辑一样，就是把事件时间改为处理时间
     *        调用trigger.onProcessingTime返回triggerResult
     *        triggerResult如果isFire直接触发窗口计算，如果isPurge清空windowState
     *        如果isCleanupTime(window, timer.getTimestamp())，清空windowState，销毁窗口
     *    emitWindowContents(W window, ACC contents)方法：
     *        转换发送windowState.get()结果，这里就是ArrayList，userFunction是InternalIterableProcessWindowFunction
     *        InternalIterableProcessWindowFunction的process方法就是调用我们传入的ProcessWindowFunction的process方法
     */
    @Test
    public void testTumblingEventTimeWindowProcessWindowFunction() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒5个，实际过去5秒
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

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

        // 事件时间窗口是左闭右开区间
        dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(processWindowFunction)
                .print();

        env.execute();
    }

    /**
     * 使用TumblingEventTimeWindows + AggregateFunction
     * 核心实现逻辑在org.apache.flink.streaming.runtime.operators.windowing.WindowOperator：
     *     和ProcessWindowFunction一样，就是windowStateDescriptor属性是AggregatingStateDescriptor，生成的windowState属性是HeapAggregatingState
     *     emitWindowContents传入的contents就是acc聚合器的rst，userFunction是InternalSingleValueWindowFunction，
     *     InternalSingleValueWindowFunction的wrappedFunction是PassThroughWindowFunction，直接发送rst到下游
     */
    @Test
    public void testTumblingEventTimeWindowAggregateFunction() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒5个，实际过去5秒
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        AggregateFunction aggregateFunction = new AggregateFunction<OnlineLog, OnlineLog, String>() {
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
        };

        // 事件时间窗口是左闭右开区间
        dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(aggregateFunction)
                .print();

        env.execute();
    }

    /**
     * 使用TumblingEventTimeWindows + AggregateFunction + ProcessWindowFunction
     * 核心实现逻辑在org.apache.flink.streaming.runtime.operators.windowing.WindowOperator：
     *     和aggregate(AggregateFunction)一样，emitWindowContents发生数据方法中userFunction也是InternalSingleValueWindowFunction，
     *     就就只是InternalSingleValueWindowFunction的wrappedFunction是我们传入的ProcessWindowFunction替换默认的PassThroughWindowFunction
     *     从InternalSingleValueWindowFunction的process方法可以看出，aggregate(AggregateFunction, ProcessWindowFunction)方法中ProcessWindowFunction传入的Iterable<String> elements就是Collections.singletonList(acc.get())，
     *     难怪官方ProcessWindowFunction代码也是直接写elements.iterator().next()获取聚合结果
     *     aggregate(AggregateFunction, ProcessWindowFunction)方法中ProcessWindowFunction收到处理的元素就是单个元素的集合，符合逻辑
     */
    @Test
    public void testTumblingEventTimeWindowAggregateFunctionWithProcessWindowFunction() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒5个，实际过去5秒
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        AggregateFunction<OnlineLog, OnlineLog, String> aggregateFunction = new AggregateFunction<OnlineLog, OnlineLog, String>() {
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
        };
        ProcessWindowFunction<String, String, Integer, TimeWindow> windowFunction = new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, ProcessWindowFunction<String, String, Integer, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                String acc = elements.iterator().next();
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                out.collect(String.format("[%s, %s):%s:%s.", windowStart, windowEnd, key, acc));
            }
        };

        // 事件时间窗口是左闭右开区间
        dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(aggregateFunction, windowFunction)
                .print();

        env.execute();
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
