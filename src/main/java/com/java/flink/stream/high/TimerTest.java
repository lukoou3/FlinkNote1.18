package com.java.flink.stream.high;

import com.java.flink.stream.func.LogMap;
import com.java.flink.stream.func.UniqueIdSequenceSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/process_function/#timers
 *
 * 两种类型的计时器（处理时间和事件时间）都由内部TimerService维护并排队执行。
 * 错误的!!!：每个key只维护一个定时时间戳，为单个key注册多个定时器，会覆盖之前的定时器。
 * 正确的：TimerService去重(键, 时间戳)的定时器，即每个键和时间戳最多有一个定时器。如果为同一个时间戳注册了多个计时器，则该onTimer()方法将只调用一次。
 * 为某个key连续设置多个定时器，这些定时器都会触发。
 * Flink 同步onTimer()和processElement()的调用。因此，用户不必担心并发修改状态。
 *
 * 容错(Fault Tolerance)：
 *    计时器具有容错性，并与应用程序的状态一起设置检查点。如果发生故障恢复或从保存点启动应用程序时，将恢复计时器。
 *    应该在恢复之前触发的检查点处理时间计时器将立即触发。当应用程序从故障中恢复或从保存点启动时，可能会发生这种情况。
 *
 * 定时器合并(Timer Coalescing):
 *   由于 Flink 只为每个键和时间戳维护一个计时器，因此您可以通过降低计时器分辨率来合并它们来减少计时器的数量。
 *   对于 1 秒的计时器分辨率（事件或处理时间），您可以将目标时间向下舍入到整秒。计时器将最多提前 1 秒触发，但不迟于毫秒精度的请求。因此，每个键和秒最多有一个计时器。
 *      val coalescedTime = ((ctx.timestamp + timeout) / 1000) * 1000
 *      ctx.timerService.registerProcessingTimeTimer(coalescedTime)
 *   由于事件时间计时器仅在水印进入时触发，您还可以使用当前的水印来安排这些计时器并将其与下一个水印合并：
 *      val coalescedTime = ctx.timerService.currentWatermark + 1
 *      ctx.timerService.registerEventTimeTimer(coalescedTime)
 *
 * 定时器也可以按如下方式停止和删除：
 *   停止处理时间计时器：ctx.timerService.deleteProcessingTimeTimer(timestampOfTimerToStop)
 *   停止事件时间计时器：ctx.timerService.deleteEventTimeTimer(timestampOfTimerToStop)
 */
public class TimerTest {

    /**
     * 不支持，只有keyed stream支持timer，运行时报错：
     * java.lang.UnsupportedOperationException: Setting timers is only supported on a keyed streams.
     */
    @Test
    public void testTimerProcess() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().enableObjectReuse();

        /**
         * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
         * 在本地使用socket测试多并行度时，一定要设置WithWatermark的Stream为1，注意Watermark的触发条件
         * 生产条件就不必考虑，毕竟实时的数据还是来的比较快的，数据量不会小
         */
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        text.process(new ProcessFunction<String, String>() {
            ArrayList<String> values = new ArrayList<String>();
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                System.out.println("currentProcessingTime:" + fmt.format(new Date(currentProcessingTime)));
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 3000);
                values.add(value);
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("onTimer:" + fmt.format(new Date(timestamp)));
                System.out.println(values);
            }
        }).print();

        env.execute();
    }

    /**
     * 经过测试KeyedProcessFunction，每个key注册的timer是独立的
     * 每个key的注册的时间只在自己的key中触发
     * registerProcessingTimeTimer在系统时间到达时触发
     * registerEventTimeTimer在Watermark时间到达时触发
     */
    @Test
    public void testTimerKeyedProcess() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().enableObjectReuse();

        /**
         * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
         * 在本地使用socket测试多并行度时，一定要设置WithWatermark的Stream为1，注意Watermark的触发条件
         * 生产条件就不必考虑，毕竟实时的数据还是来的比较快的，数据量不会小
         */
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        DataStream<Tuple3<Long, String, Integer>> ds = text.flatMap(sourceMapFunction()).setParallelism(1).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f0)
        );

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        ds.map(new LogMap<>())
                .keyBy(x -> x.f1)
                .process(new KeyedProcessFunction<String, Tuple3<Long, String, Integer>, Tuple2<String, Integer>>() {
                    ValueState<Integer> cntState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        cntState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("cnt-state", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple3<Long, String, Integer> value, KeyedProcessFunction<String, Tuple3<Long, String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int cnt = Optional.ofNullable(cntState.value()).orElse(0) + value.f2;
                        cntState.update(cnt);

                        //ctx.getCurrentKey
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        long currentWatermark = ctx.timerService().currentWatermark();
                        System.out.println("currentProcessingTime:" +  fmt.format(new Date(currentProcessingTime)));
                        System.out.println("currentWatermark:" + currentWatermark + "," + fmt.format(new Date(currentWatermark)));

                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 3000);
                        if(currentWatermark >= 0){
                            ctx.timerService().registerEventTimeTimer(currentWatermark + 1000);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<Long, String, Integer>, Tuple2<String, Integer>>.OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("currentProcessingTime_onTimer:" +  fmt.format(new Date((ctx.timerService().currentProcessingTime()))));
                        System.out.println("currentWatermark_onTimer:" +  fmt.format(new Date((ctx.timerService().currentWatermark()))));
                        System.out.println("onTimer:" +  fmt.format(new Date(timestamp)));

                        out.collect(Tuple2.of(ctx.getCurrentKey(), cntState.value()));
                    }
                }).print();

        env.execute();
    }


    /**
     * 这个例子，元素一秒产生一个，定时设置的正秒的4秒后，本来以为每个key的定时器是覆盖的，发现每个注册的定时器都会触发
     * 看下源码：
     *  [[org.apache.flink.streaming.api.SimpleTimerService]]
     *  [[org.apache.flink.streaming.api.operators.InternalTimerServiceImpl]]
     *  InternalTimerServiceImpl定时器使用队列存的：processingTimeTimersQueue属性
     */
    @Test
    public void testTimerCoalesc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DataStreamSource<LongValue> ds = env.addSource(new UniqueIdSequenceSource(1000));

        ds.keyBy(x -> 1)
                .process(new KeyedProcessFunction<Integer, LongValue, String>() {
                    @Override
                    public void processElement(LongValue value, KeyedProcessFunction<Integer, LongValue, String>.Context ctx, Collector<String> out) throws Exception {
                        TimerService service = ctx.timerService();
                        long ts = service.currentProcessingTime();
                        long time =  ts / 1000  * 1000 + 4000;
                        service.registerProcessingTimeTimer(time);
                        System.out.println(ts + "," + time + "," + value);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, LongValue, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("onTimer:" + timestamp);
                    }
                })
                .print();

        env.execute();
    }

    /**
     * 这个例子，元素一秒产生一个，定时设置的每5秒触发一次
     * 看下源码：
     *  [[org.apache.flink.streaming.api.SimpleTimerService]]
     *  [[org.apache.flink.streaming.api.operators.InternalTimerServiceImpl]]
     *  InternalTimerServiceImpl定时器使用队列存的：processingTimeTimersQueue属性
     */
    @Test
    public void testTimerCoalesc2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DataStreamSource<LongValue> ds = env.addSource(new UniqueIdSequenceSource(1000));

        ds.keyBy(x -> 1)
                .process(new KeyedProcessFunction<Integer, LongValue, String>() {
                    @Override
                    public void processElement(LongValue value, KeyedProcessFunction<Integer, LongValue, String>.Context ctx, Collector<String> out) throws Exception {
                        TimerService service = ctx.timerService();
                        long ts = service.currentProcessingTime();
                        long time =  ts / 5000  * 5000 + 5000; // 必须加上5000, 否则会立即触发
                        service.registerProcessingTimeTimer(time);
                        System.out.println(ts + "," + time + "," + value);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, LongValue, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("onTimer:" + timestamp);
                    }
                })
                .print();

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
}
