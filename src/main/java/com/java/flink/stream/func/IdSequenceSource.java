package com.java.flink.stream.func;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.LongValue;

@Deprecated
public class IdSequenceSource extends RichParallelSourceFunction<Tuple2<Integer, Long>> {
    private volatile boolean stop;
    private int indexOfSubtask;
    private long sleepMillis;

    public IdSequenceSource() {
        this(100L);
    }

    public IdSequenceSource(long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    @Override
    final public void open(Configuration parameters) throws Exception {
        indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
        long i = 0;
        int step = 1;
        while (!stop) {
            ctx.collect(Tuple2.of(indexOfSubtask, i));
            i += step;
            Thread.sleep(sleepMillis);
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
