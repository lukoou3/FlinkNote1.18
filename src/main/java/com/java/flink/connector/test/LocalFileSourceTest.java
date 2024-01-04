package com.java.flink.connector.test;

import com.java.flink.connector.localfile.LocalFileSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSourceTest {
    static final Logger LOG = LoggerFactory.getLogger(LocalFileSourceTest.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String filePath = "files/online_log.json";

        LocalFileSource<String> source = new LocalFileSource<>(filePath, 2000, Long.MAX_VALUE, Integer.MAX_VALUE, new SimpleStringSchema());
        DataStreamSource<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

        lines.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                LOG.warn(value);
            }
        });

        env.execute("LocalFileSourceTest");
    }

}
