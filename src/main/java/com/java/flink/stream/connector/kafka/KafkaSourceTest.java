package com.java.flink.stream.connector.kafka;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.LogSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * zkServer.sh start
 * zkServer.sh status
 *
 * bin/kafka-server-start.sh -daemon config/server.properties
 *
 * kafka-topics.sh --bootstrap-server 192.168.216.86:9092 --list
 *
 * kafka-topics.sh --bootstrap-server 192.168.216.86:9092 --delete --topic logs
 *
 * kafka-topics.sh --bootstrap-server 192.168.216.86:9092 --create --partitions 2 --replication-factor 1 --topic logs
 *
 * kafka-topics.sh --bootstrap-server 192.168.216.86:9092 --list
 *
 * kafka-topics.sh --bootstrap-server 192.168.216.86:9092 --describe --topic logs
 *
 * kafka-console-consumer.sh --bootstrap-server 192.168.216.86:9092 --topic logs
 *
 * kafka-consumer-groups.sh --bootstrap-server 192.168.216.86:9092 --describe --group test
 * kafka-consumer-groups.sh --bootstrap-server 192.168.216.86:9092 --describe --group test-group
 */
public class KafkaSourceTest implements Serializable {
    static final Logger LOG = LoggerFactory.getLogger(KafkaSourceTest.class);

    @Test
    public void testKafkaSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.216.86:9092");
        properties.setProperty("auto.offset.reset", "latest"); // 这个配置没用
        properties.setProperty("group.id", "test-group");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "5000");

        /**
         * 蛋疼，消费位置的配置也变了，配置committedOffsets实现之前auto.offset.reset的配置
         * 别配置 OffsetsInitializer.earliest()，这个是空最初的offset消费，不管提交的offset
         *
         * table的配置似乎也变了，EARLIEST匹配的是OffsetsInitializer.earliest()。
         * GROUP_OFFSETS匹配的是OffsetsInitializer.committedOffsets()，没提交的offset会抛出异常
         * 默认是 OffsetsInitializer.earliest()
         * [[KafkaDynamicSource#createKafkaSource]]
         *
         * 自动提交offset还是生效的
         */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setTopics("logs")
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setStartingOffsets(OffsetsInitializer.earliest()) // 显示指定, 默认earliest
                //.setStartingOffsets(OffsetsInitializer.latest()) // 显示指定
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        ds.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                LOG.warn(value);
                Thread.sleep(2000);
            }
        });

        env.execute("testKafkaSource");
    }

    @Test
    public void testKafkaSourceDeserializer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.216.86:9092");
        // properties.setProperty("auto.offset.reset", "latest"); // 这个配置没用
        properties.setProperty("group.id", "test2");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "5000");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setTopics("logs")
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaRecordDeserializationSchema<String>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws IOException {
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        Map map = new LinkedHashMap();
                        map.put("partition", record.partition());
                        map.put("offset", record.offset());
                        map.put("timestamp", record.timestamp());
                        map.put("value", value);
                        out.collect(JSON.toJSONString(map));
                    }

                    @Override
                    public TypeInformation getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .setProperties(properties)
                .build();

        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        ds.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                LOG.warn(value);
                Thread.sleep(2000);
            }
        });

        env.execute("testKafkaSource");
    }
}
