package com.java.flink.stream.func;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalTextFileUseMmapSourceFunctionV2Test {
    static final Logger LOG = LoggerFactory.getLogger(LocalTextFileUseMmapSourceFunctionV2Test.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String filePath = "files/online_log.json";
        DataStream<String> lines = env.addSource(new LocalTextFileUseMmapSourceFunctionV2(filePath, 2000, Long.MAX_VALUE, Integer.MAX_VALUE));

        lines.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                LOG.warn(value);
            }
        });

        env.execute("LocalTextFileUseMmapSourceFunctionV2Test");
    }

}
