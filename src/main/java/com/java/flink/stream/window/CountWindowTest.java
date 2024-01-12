package com.java.flink.stream.window;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CountWindowTest {
    String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":0, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    String[] fieldGenesDesc2 = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":1}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":0, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    /**
     * keyBy(x -> x.pageId)后countWindow(5)，每个key每到达5个元素，触发窗口计算。每个key不会同时触发，不像timeWindow，因为timeWindow触发条件的时间对于每个key是一样的，会同时触发。
     *
     * countWindow(size)的逻辑，实际就是一个GlobalWindows(默认NeverTrigger)加上CountTrigger：
     *     public WindowedStream<T, KEY, GlobalWindow> countWindow(long size) {
     *         return window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
     *     }
     *
     * 注意：这里使用的是PurgingTrigger.of(CountTrigger.of(size))，不是CountTrigger.of(size)，每次触发都会清空之前的状态
     *
     * WindowAssigner有个抽象方法getDefaultTrigger返回Trigger(默认的Trigger)：
     *      TumblingProcessingTimeWindows返回：ProcessingTimeTrigger
     *      SlidingProcessingTimeWindows返回：ProcessingTimeTrigger
     *      TumblingEventTimeWindows返回：EventTimeTrigger
     *      SlidingEventTimeWindows返回：EventTimeTrigger
     *      GlobalWindows返回：NeverTrigger
     *
     */
    @Test
    public void testCountWindowTumbling() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .countWindow(5)
                .process(new ProcessWindowFunction<OnlineLog, String, Integer, GlobalWindow>() {
                    @Override
                    public void process(Integer key, ProcessWindowFunction<OnlineLog, String, Integer, GlobalWindow>.Context context, Iterable<OnlineLog> elements, Collector<String> out) throws Exception {
                        int visitCnt = 0;
                        for (OnlineLog element : elements) {
                            visitCnt += element.visitCnt;
                        }
                        // GlobalWindow中没有其他的信息，不像时间窗口的元数据多
                        String window = context.window().toString();
                        out.collect(String.format("%s:%s:%s", window, key, visitCnt));
                    }
                })
                .print();

        env.execute();
    }

    /**
     * 这里使用的是PurgingTrigger.of(CountTrigger.of(size))，不是CountTrigger.of(size)，每次触发都会清空之前的状态
     * 每个窗口开始聚合时会重新调用createAccumulator()，之前的状态被清除了
     * org.apache.flink.runtime.state.heap.HeapAggregatingState.AggregateTransformation#apply
     * 之后可以看看window的实现原理，看看怎么实现window的状态的
     */
    @Test
    public void testCountWindowTumblingAggregate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc2, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .countWindow(5)
                .aggregate(new AggregateFunction<OnlineLog, Tuple3<Integer, Long, Long>, String>() {
                    @Override
                    public Tuple3<Integer, Long, Long> createAccumulator() {
                        return Tuple3.of(0, Long.MAX_VALUE, Long.MIN_VALUE);
                    }

                    @Override
                    public Tuple3<Integer, Long, Long> add(OnlineLog value, Tuple3<Integer, Long, Long> acc) {
                        return Tuple3.of(value.visitCnt + acc.f0, Math.min(acc.f1, value.time), Math.max(acc.f2, value.time));
                    }

                    @Override
                    public String getResult(Tuple3<Integer, Long, Long> accumulator) {
                        return accumulator.toString();
                    }

                    @Override
                    public Tuple3<Integer, Long, Long> merge(Tuple3<Integer, Long, Long> a, Tuple3<Integer, Long, Long> b) {
                        return null;
                    }
                }, new ProcessWindowFunction<String, String, Integer, GlobalWindow>() {
                    @Override
                    public void process(Integer key, ProcessWindowFunction<String, String, Integer, GlobalWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        String rst = elements.iterator().next();
                        out.collect(String.format("%s:%s:%s", context.window().toString(), key, rst));
                    }
                })
                .print();

        env.execute();
    }

    /**
     * countWindow(size)的逻辑，实际就是一个GlobalWindows(默认NeverTrigger)加上CountEvictor和CountTrigger：
     *     public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
     *         return window(GlobalWindows.create())
     *                 .evictor(CountEvictor.of(size))
     *                 .trigger(CountTrigger.of(slide));
     *     }
     *
     *  CountEvictor会删除前面多余的元素，它这个怎么应用聚合函数呢
     */
    @Test
    public void testCountWindowSliding() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .countWindow(5, 2)
                .process(new ProcessWindowFunction<OnlineLog, String, Integer, GlobalWindow>() {
                    @Override
                    public void process(Integer key, ProcessWindowFunction<OnlineLog, String, Integer, GlobalWindow>.Context context, Iterable<OnlineLog> elements, Collector<String> out) throws Exception {
                        int visitCnt = 0;
                        long minTime = Long.MAX_VALUE;
                        long maxTime = Long.MIN_VALUE;
                        for (OnlineLog element : elements) {
                            visitCnt += element.visitCnt;
                            minTime = Math.min(minTime, element.time);
                            maxTime = Math.max(maxTime, element.time);
                        }
                        // GlobalWindow中没有其他的信息，不像时间窗口的元数据多
                        String window = context.window().toString();
                        out.collect(String.format("%s:%s:%s:%d-%d", window, key, visitCnt, minTime,maxTime));
                    }
                })
                .print();

        env.execute();
    }

    /**
     * 使用的是EvictingWindowOperator，使用AggregateFunction没有优化，之前还是会缓存每个元素list
     * org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator#emitWindowContents
     * 所以说不要使用evictor，效率低下。
     * 想想也知道，它这个可以实现剔除元素，但是没有定义删除元素聚合对象删除元素的方法，怎么能高效实现呢。
     *
     * 其他人也发现了这个问题：flink ProcessWindowFunction使用心得(https://www.jianshu.com/p/7918ebe165bf)
     *     flink中的ProcessWindowFunction经常用在窗口触发后对结果的数据的迭代处理以及获得窗口的开始时间和截止时间等操作。它可以结合reduce,aggregate等一起使用。但是使用过程中需要注意：
     *      1.如果ProcessWindowFunction没有结合reduce,aggregate等其他窗口函数来计算的话，他是会缓存落入该窗口的所有数据，等待窗口触发的时候再一起执行ProcessWindowFunction中的process方法的，如果数据量太大很可能会导致OOM，
     *      建议在ProcessWindowFunction之前加入一个reduce，aggregate等算子，这些算子会在数据落入窗口的时候就执行reduce等操作，而不是缓存直到窗口触发执行的时候才进行reduce操作，从而避免了缓存所有窗口数据，
     *      窗口触发的时候ProcessWindowFunction拿到的只是reduce操作后的结果。
     *
     *      2.在window中使用Evictor操作的时候，无论window是否有reduce等其他算子，window一律缓存窗口的所有数据，等到窗口触发的时候先执行evictor方法，再执行reduce，最后再执行ProcessWindowFunction操作，
     *      源代码是通过EvictingWindowOperator这个类来实现的对比没有Evictor时效率低了很多，如果窗口缓存的数据量很大的话也会导致OOM的发生，使用Evictor时要谨慎！
     *
     */
    @Test
    public void testCountWindowSlidingAggregate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc2, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .keyBy(x -> x.pageId)
                .countWindow(5, 2)
                .aggregate(new AggregateFunction<OnlineLog, Tuple3<Integer, Long, Long>, String>() {
                    @Override
                    public Tuple3<Integer, Long, Long> createAccumulator() {
                        return Tuple3.of(0, Long.MAX_VALUE, Long.MIN_VALUE);
                    }

                    @Override
                    public Tuple3<Integer, Long, Long> add(OnlineLog value, Tuple3<Integer, Long, Long> acc) {
                        return Tuple3.of(value.visitCnt + acc.f0, Math.min(acc.f1, value.time), Math.max(acc.f2, value.time));
                    }

                    @Override
                    public String getResult(Tuple3<Integer, Long, Long> accumulator) {
                        return accumulator.toString();
                    }

                    @Override
                    public Tuple3<Integer, Long, Long> merge(Tuple3<Integer, Long, Long> a, Tuple3<Integer, Long, Long> b) {
                        return null;
                    }
                }, new ProcessWindowFunction<String, String, Integer, GlobalWindow>() {
                    @Override
                    public void process(Integer key, ProcessWindowFunction<String, String, Integer, GlobalWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        String rst = elements.iterator().next();
                        out.collect(String.format("%s:%s:%s", context.window().toString(), key, rst));
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
