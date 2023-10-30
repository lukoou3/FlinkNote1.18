package com.java.flink.stream.func;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogMap<T> extends RichMapFunction<T, T> {
    static final Logger LOG = LoggerFactory.getLogger(LogSink.class);

    private String prefix;
    private long logIntervalMs;
    private long lastLogTs;

    public LogMap() {
    }

    public LogMap(long logIntervalMs) {
        this.logIntervalMs = logIntervalMs;
    }

    public LogMap(String prefix, long logIntervalMs) {
        this.prefix = prefix;
        this.logIntervalMs = logIntervalMs;
    }

    @Override
    public T map(T value) throws Exception {
        if(logIntervalMs > 0L){
            long ts = System.currentTimeMillis();
            if(ts - lastLogTs >= logIntervalMs){
                log(value);
                lastLogTs = ts;
            }
        }else{
            log(value);
        }

        return value;
    }

    private void log(T value){
        if(StringUtils.isNotBlank(prefix)){
            LOG.info("{}:{}" , prefix, value.toString());
        }else{
            LOG.info(value.toString());
        }
    }
}
