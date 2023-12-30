package com.java.flink.stream.connector.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * FlinkKafkaConsumer is deprecated and will be removed with Flink 1.17, please use KafkaSource instead.
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/datastream/kafka/#kafka-sourcefunction
 */
public class FlinkKafkaConsumerTest {
    static final Logger LOG = LoggerFactory.getLogger(KafkaSourceTest.class);

    @Test
    public void testFlinkKafkaConsumer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.216.86:9092");
        //properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", "test33");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "5000");

        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("logs", new SimpleStringSchema(), properties);

        DataStreamSource ds = env.addSource(kafkaConsumer);

        ds.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                LOG.warn(value);
                Thread.sleep(2000);
            }
        });

        env.execute("testFlinkKafkaConsumer");
    }

}
