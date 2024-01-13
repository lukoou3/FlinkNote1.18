package com.java.flink.stream.window;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class ConsecutiveWindowTest {
    String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":500, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    /**
     * flink中数据元素和watermark的传递是单独的，所以经过window处理，不影响之后stream的watermark
     * 而且到达首次触发window的watermark时，会先触发window发生数据元素，然后在发送这个watermark，所以可以连续使用多个窗口实现window topn
     * org.apache.flink.streaming.api.operators.AbstractStreamOperator.processWatermark处理watermark的逻辑：
     *    public void processWatermark(Watermark mark) throws Exception {
     *        // 触发事件时间定时器
     *        if (timeServiceManager != null) {
     *            // window触发发送元素也是在这里
     *            timeServiceManager.advanceWatermark(mark);
     *        }
     *        // 向下游发送watermark
     *        output.emitWatermark(mark);
     *    }
     * 官网中有类似的描述：
     *   https://nightlies.apache.org/flink/flink-docs-release-1.18/zh/docs/dev/datastream/operators/windows/#interaction-of-watermarks-and-windows
     *   当 watermark 到达窗口算子时，它触发了两件事：
     *       这个 watermark 触发了所有最大 timestamp（即 end-timestamp - 1）小于它的窗口
     *       这个 watermark 被原封不动地转发给下游的任务。
     *   通俗来讲，watermark 将当前算子中那些“一旦这个 watermark 被下游任务接收就肯定会就超时”的窗口全部冲走。
     *
     *   https://nightlies.apache.org/flink/flink-docs-release-1.18/zh/docs/dev/datastream/operators/windows/#consecutive-windowed-operations
     *   如之前提到的，窗口结果的 timestamp 如何计算以及 watermark 如何与窗口相互作用使串联多个窗口操作成为可能。 这提供了一种便利的方法，让你能够有两个连续的窗口，他们即能使用不同的 key， 又能让上游操作中某个窗口的数据出现在下游操作的相同窗口。参考下例：
     *       DataStream<Integer> input = ...;
     *
     *       DataStream<Integer> resultsPerKey = input
     *       .keyBy(<key selector>)
     *       .window(TumblingEventTimeWindows.of(Time.seconds(5)))
     *       .reduce(new Summer());
     *
     *       DataStream<Integer> globalResults = resultsPerKey
     *       .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
     *       .process(new TopKWindowFunction());
     *   这个例子中，第一个操作中时间窗口[0, 5) 的结果会出现在下一个窗口操作的 [0, 5) 窗口中。 这就可以让我们先在一个窗口内按 key 求和，再在下一个操作中找出这个窗口中 top-k 的元素。
     */
    @Test
    public void testWindowEmitWatermark() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒5个，实际过去5秒
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow> processWindowFunction = new ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>.Context context, Iterable<OnlineLog> elements, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
                int visitCnt = 0;
                for (OnlineLog element : elements) {
                    visitCnt += element.visitCnt;
                }
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                // 只发前几个窗口的数据
                if (context.window().getStart() < 15000) {
                    // 不往下游发生数据，不影响watermark照样传递
                    out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
                }
            }
        };

        DataStream<Tuple4<Integer, String, String, Integer>> resultsPerKey = dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(processWindowFunction);

        resultsPerKey.keyBy(x -> 1)
                .process(new KeyedProcessFunction<Integer, Tuple4<Integer, String, String, Integer>, String>() {

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Integer> value, KeyedProcessFunction<Integer, Tuple4<Integer, String, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println(value + ", Watermark:" + ctx.timerService().currentWatermark());
                        if (ctx.timerService().currentWatermark() > 0) {
                            long ts = ctx.timerService().currentWatermark();
                            ts = ts / 5000 * 5000 + 5000;
                            ctx.timerService().registerEventTimeTimer(ts);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, Tuple4<Integer, String, String, Integer>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("onTimer:" + timestamp + ", Watermark:" + ctx.timerService().currentWatermark());
                        timestamp = timestamp / 5000 * 5000 + 5000;
                        ctx.timerService().registerEventTimeTimer(timestamp);
                    }
                })
                .print();

        env.execute();
    }

    /**
     * DataStream api实现window topn也是比较简单的，连续使用两个window即可，这里第一个window使用的ProcessWindowFunction就是为了测试，实际可以使用聚合函数。
     */
    @Test
    public void testEventTimeWindowTopN() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒2个，5秒10个，实际过去5秒
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow> processWindowFunction = new ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>.Context context, Iterable<OnlineLog> elements, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
                int visitCnt = 0;
                for (OnlineLog element : elements) {
                    visitCnt += element.visitCnt;
                }
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
                out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
            }
        };

        DataStream<Tuple4<Integer, String, String, Integer>> resultsPerKey = dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(processWindowFunction);

        resultsPerKey.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Tuple4<Integer, String, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple4<Integer, String, String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple4<Integer, String, String, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("************window:" + context.window().getStart() + "," + context.window().getEnd() + "*************");
                        List<Tuple4<Integer, String, String, Integer>> list = (List<Tuple4<Integer, String, String, Integer>>) elements;
                        Collections.sort(list, new Comparator<Tuple4<Integer, String, String, Integer>>() {
                            @Override
                            public int compare(Tuple4<Integer, String, String, Integer> o1, Tuple4<Integer, String, String, Integer> o2) {
                                return Integer.compare(o2.f3, o1.f3);
                            }
                        });
                        for (int i = 0; i < list.size(); i++) {
                            System.out.println(list.get(i));
                        }
                        System.out.println("************end*************");
                        System.out.println();
                    }
                })
                .print();

        env.execute();
    }

    @Test
    public void testWindowEmitProcessingTime() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒5个，实际过去5秒
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsNoWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class));

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow> processWindowFunction = new ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>.Context context, Iterable<OnlineLog> elements, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
                int visitCnt = 0;
                for (OnlineLog element : elements) {
                    visitCnt += element.visitCnt;
                }
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
                out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
            }
        };

        DataStream<Tuple4<Integer, String, String, Integer>> resultsPerKey = dsNoWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(processWindowFunction);

        resultsPerKey.keyBy(x -> 1)
                .process(new KeyedProcessFunction<Integer, Tuple4<Integer, String, String, Integer>, String>() {

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Integer> value, KeyedProcessFunction<Integer, Tuple4<Integer, String, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println(value + ", currentProcessingTime:" + fmt.format(new Date(ctx.timerService().currentProcessingTime())));
                    }
                })
                .print();

        env.execute();
    }

    /**
     * ProcessingTime是无法通过连续使用两个window实现window topn的，因为ProcessingTime是不用传递的，直接取的系统时间
     * 注册ProcessingTime，是通过java.util.concurrent.ScheduledThreadPoolExecutor计算延时调度的
     * 处理时间的注册：org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService.registerTimer
     * 那他这个怎么保证每个key注册的time是去重的呢?
     */
    @Test
    public void testProcessingTimeWindowTopN() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒10个，实际过去5秒
        env.setParallelism(2);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsNoWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class));

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow> processWindowFunction = new ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, ProcessWindowFunction<OnlineLog, Tuple4<Integer, String, String, Integer>, Integer, TimeWindow>.Context context, Iterable<OnlineLog> elements, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
                int visitCnt = 0;
                for (OnlineLog element : elements) {
                    visitCnt += element.visitCnt;
                }
                String windowStart = fmt.format(new Date(context.window().getStart()));
                String windowEnd = fmt.format(new Date(context.window().getEnd()));
                System.out.println("window1:" + Tuple4.of(key, windowStart, windowEnd, visitCnt));
                out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
                out.collect(Tuple4.of(key, windowStart, windowEnd, visitCnt));
            }
        };

        DataStream<Tuple4<Integer, String, String, Integer>> resultsPerKey = dsNoWatermark.map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(processWindowFunction);

        resultsPerKey.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Tuple4<Integer, String, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple4<Integer, String, String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple4<Integer, String, String, Integer>> elements, Collector<String> out) throws Exception {
                        System.out.println("************window:" + fmt.format(context.window().getStart()) + "," + fmt.format(context.window().getEnd()) + "*************");
                        List<Tuple4<Integer, String, String, Integer>> list = (List<Tuple4<Integer, String, String, Integer>>) elements;
                        Collections.sort(list, new Comparator<Tuple4<Integer, String, String, Integer>>() {
                            @Override
                            public int compare(Tuple4<Integer, String, String, Integer> o1, Tuple4<Integer, String, String, Integer> o2) {
                                return Integer.compare(o2.f3, o1.f3);
                            }
                        });
                        for (int i = 0; i < list.size(); i++) {
                            System.out.println(list.get(i));
                        }
                        System.out.println("************end*************");
                        System.out.println();
                    }
                })
                .print();

        env.execute();
    }


    /**
     * ProcessingTime每个key每个时间戳也是同时只能注册一个：
     *   org.apache.flink.streaming.api.operators.InternalTimerServiceImpl.registerProcessingTimeTimer
     *   注册处理时间，先把ts添加到processingTimeTimersQueue(HeapPriorityQueueSet)，这个是去重的，重复加就不会调用processingTimeService.registerTimer(time, this::onProcessingTime)
     *
     */
    @Test
    public void testProcessingTimeTimer() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒10个，实际过去5秒
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> 1)
                .process(new KeyedProcessFunction<Integer, OnlineLog, String>() {
                    @Override
                    public void processElement(OnlineLog value, KeyedProcessFunction<Integer, OnlineLog, String>.Context ctx, Collector<String> out) throws Exception {
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        currentProcessingTime = currentProcessingTime / 5000 * 5000 + 5000;
                        System.out.println(currentProcessingTime);
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, OnlineLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("key:" + ctx.getCurrentKey() + ",currentProcessingTime_onTimer:" + fmt.format(new Date((ctx.timerService().currentProcessingTime()))));
                    }
                })
                .print();


        env.execute();
    }

    /**
     * EventTime每个key每个时间戳也是同时只能注册一个：
     *   org.apache.flink.streaming.api.operators.InternalTimerServiceImpl.registerEventTimeTimer
     *   注册事件时间，先把ts添加到eventTimeTimersQueue(HeapPriorityQueueSet)，这个是去重的
     *   InternalTimerServiceImpl registerProcessingTimeTimer和registerEventTimeTimer方法的区别：
     *        处理时间借用HeapPriorityQueueSet去重，然后使用调度器注册延时调度
     *        事件时间存入HeapPriorityQueueSet，当收到新的watermark时，会从eventTimeTimersQueue查看是否有事件触发
     *        收到watermark：调用
     *          org.apache.flink.streaming.api.operators.AbstractStreamOperator.processWatermark =>
     *          org.apache.flink.streaming.api.operators.InternalTimerServiceImpl.advanceWatermark:从eventTimeTimersQueue取时间查看
     */
    @Test
    public void testEventTimeTimer() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每秒1个，5秒5个，实际过去5秒
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        DataStream<OnlineLog> dsWithWatermark = ds.map(x -> JSON.parseObject(x, OnlineLog.class)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OnlineLog>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.time)
        );
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        dsWithWatermark.map(new LogMap<>())
                .keyBy(x -> 1)
                .process(new KeyedProcessFunction<Integer, OnlineLog, String>() {
                    @Override
                    public void processElement(OnlineLog value, KeyedProcessFunction<Integer, OnlineLog, String>.Context ctx, Collector<String> out) throws Exception {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        if (currentWatermark > 0) {
                            currentWatermark = currentWatermark / 5000 * 5000 + 5000;
                            System.out.println(currentWatermark);
                            ctx.timerService().registerEventTimeTimer(currentWatermark);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, OnlineLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("key:" + ctx.getCurrentKey() + ",currentWatermark_onTimer:" + timestamp + "," + ctx.timerService().currentWatermark());
                    }
                })
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
