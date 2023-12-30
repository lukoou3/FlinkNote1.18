package com.java.flink.stream.connector.kafka;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaSinkTest {
    static final Logger LOG = LoggerFactory.getLogger(KafkaSinkTest.class);

    @Test
    public void testKafkaSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DataStream<Long> ds = env.fromSource(new NumberSequenceSource(0, Long.MAX_VALUE), WatermarkStrategy.noWatermarks(), "Sequence Source");
        SingleOutputStreamOperator<String> rstDs = ds.map(id -> {
            Thread.sleep(2000);
            Map map = new LinkedHashMap();
            map.put("id", id);
            map.put("ts", System.currentTimeMillis());
            map.put("date_time", new Timestamp(System.currentTimeMillis()).toString());
            return JSON.toJSONString(map);
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.216.86:9092");

        /**
         * KafkaRecordSerializationSchema的默认FlinkKafkaPartitioner是null，这个就比之前的FlinkKafkaProducer好点
         *
         * org.apache.flink.connector.kafka.sink.KafkaWriter#write:
         *     新版本的这个不会发送KafkaRecordSerializationSchema产生的null ProducerRecord
         *     可惜默认KafkaRecordSerializationSchemaWrapper的serialize方法不会返回null，这个感觉可以定制一个。防止SerializationSchema产生异常，但是不想停止程序。
         */
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("logs")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(properties)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        rstDs.sinkTo(sink);

        env.execute("KafkaSinkTest");
    }

    @Test
    public void testKafkaSinkPartitioner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DataStream<Long> ds = env.fromSource(new NumberSequenceSource(0, Long.MAX_VALUE), WatermarkStrategy.noWatermarks(), "Sequence Source");
        SingleOutputStreamOperator<String> rstDs = ds.map(id -> {
            Thread.sleep(2000);
            Map map = new LinkedHashMap();
            map.put("id", id);
            map.put("ts", System.currentTimeMillis());
            map.put("date_time", new Timestamp(System.currentTimeMillis()).toString());
            return JSON.toJSONString(map);
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.216.86:9092");

        /**
         * KafkaRecordSerializationSchema的默认FlinkKafkaPartitioner是null，这个就比之前的FlinkKafkaProducer好点
         * 设置成FlinkKafkaProducer默认的FlinkFixedPartitioner，也可以自定义分区器
         */
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("logs")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(new FlinkFixedPartitioner<String>())
                        .build()
                )
                .setKafkaProducerConfig(properties)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        rstDs.sinkTo(sink);

        env.execute("KafkaSinkTest");
    }

}
