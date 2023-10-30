package com.java.flink.stream.func;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSink<T> extends RichSinkFunction<T> {
    static final Logger LOG = LoggerFactory.getLogger(LogSink.class);

    @Override
    public void invoke(T value, Context context) throws Exception {
        LOG.info(value.toString());
    }
}
