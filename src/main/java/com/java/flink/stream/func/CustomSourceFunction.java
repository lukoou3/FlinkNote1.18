package com.java.flink.stream.func;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;

// SourceFunction这个接口废弃了，建议使用Source接口，之后看看怎么实现
@Deprecated
public abstract class CustomSourceFunction<T> extends RichParallelSourceFunction<T> {
    private volatile boolean stop;
    protected int indexOfSubtask;


    @Override
    final public void open(Configuration parameters) throws Exception {
        indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (!stop) {
            ctx.collect(elementGene());
        }
    }

    public abstract T elementGene();

    @Override
    public void cancel() {
        stop = true;
    }
}
