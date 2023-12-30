package com.java.flink.stream.connector.kafka;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class FlinkKafkaProducerTest {
    static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerTest.class);

    @Test
    public void testFlinkKafkaProducer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.216.86:9092");

        /**
         * 默认分区器是FlinkFixedPartitioner：parallelInstanceId % partitions.length
         * 并行度是1的话，只有一个sub task，只向Kafka的0分区发数据。
         */
        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("logs", new SimpleStringSchema(), properties);

        DataStream<Long> ds = env.fromSource(new NumberSequenceSource(0, Long.MAX_VALUE), WatermarkStrategy.noWatermarks(), "Sequence Source");
        SingleOutputStreamOperator<String> rstDs = ds.map(id -> {
            Thread.sleep(2000);
            Map map = new LinkedHashMap();
            map.put("id", id);
            map.put("ts", System.currentTimeMillis());
            map.put("date_time", new Timestamp(System.currentTimeMillis()).toString());
            return JSON.toJSONString(map);
        });

        rstDs.addSink(kafkaProducer);

        env.execute("FlinkKafkaProducerTest");
    }

    @Test
    public void testFlinkKafkaPartitioner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.216.86:9092");

        /**
         * 默认分区器是FlinkFixedPartitioner：parallelInstanceId % partitions.length
         * 正常情况都不应该使用默认的FlinkFixedPartitioner，有可能数据倾斜
         * 设置Optional.empty()后，就不会使用分区器了，默认kafka的轮训分区
         */
        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("logs",
                new SimpleStringSchema(), properties, Optional.empty());

        DataStream<Long> ds = env.fromSource(new NumberSequenceSource(0, Long.MAX_VALUE), WatermarkStrategy.noWatermarks(), "Sequence Source");
        SingleOutputStreamOperator<String> rstDs = ds.map(id -> {
            Thread.sleep(2000);
            Map map = new LinkedHashMap();
            map.put("id", id);
            map.put("ts", System.currentTimeMillis());
            map.put("date_time", new Timestamp(System.currentTimeMillis()).toString());
            return JSON.toJSONString(map);
        });

        rstDs.addSink(kafkaProducer);

        env.execute("FlinkKafkaProducerTest");
    }
}
