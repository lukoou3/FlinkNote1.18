package com.java.flink.connector.log;

import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static com.java.flink.connector.log.LogMode.LOG_INFO;
import static com.java.flink.connector.log.LogMode.STDOUT;

public class LogSinkFunction<T> extends RichSinkFunction<T> {
    static final Logger LOG = LoggerFactory.getLogger(LogSinkFunction.class);

    private final LogMode logMode;
    private final SerializationSchema<T> serializer;

    public LogSinkFunction(LogMode logMode, SerializationSchema<T> serializer) {
        this.logMode = logMode;
        this.serializer = serializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        serializer.open(RuntimeContextInitializationContextAdapters.serializationAdapter(getRuntimeContext()));
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        byte[] bytes = serializer.serialize(value);
        String msg = new String(bytes, StandardCharsets.UTF_8);
        if(value instanceof Row){
            msg = ((Row)value).getKind().toString() + ": " + msg;
        } else if (value instanceof RowData) {
            msg = ((RowData)value).getRowKind().toString() + ": " + msg;
        }
        if(logMode == STDOUT){
            System.out.println(msg);
        } else if (logMode == LOG_INFO) {
            LOG.info(msg);
        }else{
            LOG.warn(msg);
        }
    }
}
