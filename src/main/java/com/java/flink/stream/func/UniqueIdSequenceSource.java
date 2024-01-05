package com.java.flink.stream.func;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.LongValue;

public class UniqueIdSequenceSource extends RichParallelSourceFunction<LongValue> {
    private volatile boolean stop;
    private int n;
    private int k;
    private long sleepMillis;

    public UniqueIdSequenceSource() {
        this(100L);
    }

    public UniqueIdSequenceSource(long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    @Override
    final public void open(Configuration parameters) throws Exception {
        n = getRuntimeContext().getNumberOfParallelSubtasks();
        k = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<LongValue> ctx) throws Exception {
        long i = k;
        int step = n;
        while (!stop) {
            ctx.collect(new LongValue(i));
            i += step;
            Thread.sleep(sleepMillis);
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
