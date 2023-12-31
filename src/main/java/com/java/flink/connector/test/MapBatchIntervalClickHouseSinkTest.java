package com.java.flink.connector.test;

import com.alibaba.fastjson2.JSON;
import com.java.flink.connector.clickhouse.sink.MapBatchIntervalClickHouseSink;
import com.java.flink.stream.func.FieldGeneSouce;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/**
CREATE TABLE IF NOT EXISTS test.test_ck_sink_local (
    id Int64,
    datetime DateTime,
    datetime64 DateTime64(3),
    ts UInt32,
    ts_ms Int64,
    insert_time Int64 MATERIALIZED toUnixTimestamp(now()),
    int64 Int64,
    uint64 UInt64,
    int32 Int32,
    uint32 UInt32,
    int32_nullalbe Nullable(Int32),
    str String,
    str_dict_encoded LowCardinality(String),
    int32_list Array(Int32),
    int64_list Array(Int64),
    str_list Array(String)
)
ENGINE=MergeTree
PARTITION BY toYYYYMMDD(datetime)
ORDER BY (datetime, id)
;

show create table test.test_ck_sink_local;

 [
 {"type":"long_inc", "fields":{"name":"id", "start":1}},
 {"type":"long_inc", "fields":{"name":"datetime", "start":1701360000000}},
 {"type":"long_inc", "fields":{"name":"datetime64", "start":1701360000000}},
 {"type":"long_inc_batch", "fields":{"name":"ts", "start":1701360000, "batch":1000}},
 {"type":"long_inc", "fields":{"name":"ts_ms", "start":1701360000000}},
 {"type":"long_random", "fields":{"name":"int64", "start":1, "end":1000000000}},
 {"type":"int_random", "fields":{"name":"uint64", "start":1, "null_ratio":0.1}},
 {"type":"int_random", "fields":{"name":"int32", "start":1, "null_ratio":0.1}},
 {"type":"int_random", "fields":{"name":"uint32", "start":1, "null_ratio":0.1}},
 {"type":"int_random", "fields":{"name":"int32_nullalbe", "start":1, "null_ratio":0.1}},
 {"type":"str_from_list", "fields":{"name":"str","values":["a", "b", "莫南", "燕青丝", null]}},
 {"type":"str_from_list", "fields":{"name":"str_dict_encoded","values":["a", "b", "莫南", "燕青丝"]}},
 {"type":"str_list_from_list", "fields":{"name":"int32_list","values":["1", "2", "3", "4"]}},
 {"type":"str_list_from_list", "fields":{"name":"int64_list","values":["1", "2", "3", "4"]}},
 {"type":"str_list_from_list", "fields":{"name":"str_list","values":["a", "b", "莫南", "燕青丝"]}}
 ]

[
    {"type":"long_inc", "fields":{"name":"id", "start":1}},
    {"type":"long_inc", "fields":{"name":"datetime", "start":1701360000000}},
    {"type":"long_inc", "fields":{"name":"datetime64", "start":1701360000000}},
    {"type":"long_inc_batch", "fields":{"name":"ts", "start":1701360000, "batch":1000}},
    {"type":"long_inc", "fields":{"name":"ts_ms", "start":1701360000000}},
    {"type":"long_random", "fields":{"name":"int64", "start":1, "end":1000000000}},
    {"type":"int_random", "fields":{"name":"uint64", "start":1, "null_ratio":0.1}},
    {"type":"int_random", "fields":{"name":"int32", "start":1, "null_ratio":0.1}},
    {"type":"int_random", "fields":{"name":"uint32", "start":1, "null_ratio":0.1}},
    {"type":"int_random", "fields":{"name":"int32_nullalbe", "start":1, "null_ratio":0.1}},
    {"type":"str_from_list", "fields":{"name":"str","values":["a", "b", "莫南", "燕青丝", null]}},
    {"type":"str_from_list", "fields":{"name":"str_dict_encoded","values":["a", "b", "莫南", "燕青丝", null]}},
    {"type":"str_list_from_list", "fields":{"name":"int32_list","values":["1", "2", "3", "4", null]}},
    {"type":"str_list_from_list", "fields":{"name":"int64_list","values":["1", "2", "3", "4", null]}},
    {"type":"str_list_from_list", "fields":{"name":"str_list","values":["a", "b", "莫南", "燕青丝", null]}}
]

 */
public class MapBatchIntervalClickHouseSinkTest {

    @Test
    public void testClickHouseSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        String fieldGenesDesc = "[{\"type\":\"long_inc\",\"fields\":{\"name\":\"id\",\"start\":1}},{\"type\":\"long_inc\",\"fields\":{\"name\":\"datetime\",\"start\":1701360000000}},{\"type\":\"long_inc\",\"fields\":{\"name\":\"datetime64\",\"start\":1701360000000}},{\"type\":\"long_inc_batch\",\"fields\":{\"name\":\"ts\",\"start\":1701360000,\"batch\":1000}},{\"type\":\"long_inc\",\"fields\":{\"name\":\"ts_ms\",\"start\":1701360000000}},{\"type\":\"long_random\",\"fields\":{\"name\":\"int64\",\"start\":1,\"end\":1000000000}},{\"type\":\"int_random\",\"fields\":{\"name\":\"uint64\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"int_random\",\"fields\":{\"name\":\"int32\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"int_random\",\"fields\":{\"name\":\"uint32\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"int_random\",\"fields\":{\"name\":\"int32_nullalbe\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"str_from_list\",\"fields\":{\"name\":\"str\",\"values\":[\"a\",\"b\",\"莫南\",\"燕青丝\",null]}},{\"type\":\"str_from_list\",\"fields\":{\"name\":\"str_dict_encoded\",\"values\":[\"a\",\"b\",\"莫南\",\"燕青丝\",null]}},{\"type\":\"str_list_from_list\",\"fields\":{\"name\":\"int32_list\",\"values\":[\"1\",\"2\",\"3\",\"4\",null]}},{\"type\":\"str_list_from_list\",\"fields\":{\"name\":\"int64_list\",\"values\":[\"1\",\"2\",\"3\",\"4\",null]}},{\"type\":\"str_list_from_list\",\"fields\":{\"name\":\"str_list\",\"values\":[\"a\",\"b\",\"莫南\",\"燕青丝\",null]}}]";
        String fieldGenesDescListNotNullEle = "[{\"type\":\"long_inc\",\"fields\":{\"name\":\"id\",\"start\":1}},{\"type\":\"long_inc\",\"fields\":{\"name\":\"datetime\",\"start\":1701360000000}},{\"type\":\"long_inc\",\"fields\":{\"name\":\"datetime64\",\"start\":1701360000000}},{\"type\":\"long_inc_batch\",\"fields\":{\"name\":\"ts\",\"start\":1701360000,\"batch\":1000}},{\"type\":\"long_inc\",\"fields\":{\"name\":\"ts_ms\",\"start\":1701360000000}},{\"type\":\"long_random\",\"fields\":{\"name\":\"int64\",\"start\":1,\"end\":1000000000}},{\"type\":\"int_random\",\"fields\":{\"name\":\"uint64\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"int_random\",\"fields\":{\"name\":\"int32\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"int_random\",\"fields\":{\"name\":\"uint32\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"int_random\",\"fields\":{\"name\":\"int32_nullalbe\",\"start\":1,\"null_ratio\":0.1}},{\"type\":\"str_from_list\",\"fields\":{\"name\":\"str\",\"values\":[\"a\",\"b\",\"莫南\",\"燕青丝\",null]}},{\"type\":\"str_from_list\",\"fields\":{\"name\":\"str_dict_encoded\",\"values\":[\"a\",\"b\",\"莫南\",\"燕青丝\"]}},{\"type\":\"str_list_from_list\",\"fields\":{\"name\":\"int32_list\",\"values\":[\"1\",\"2\",\"3\",\"4\"]}},{\"type\":\"str_list_from_list\",\"fields\":{\"name\":\"int64_list\",\"values\":[\"1\",\"2\",\"3\",\"4\"]}},{\"type\":\"str_list_from_list\",\"fields\":{\"name\":\"str_list\",\"values\":[\"a\",\"b\",\"莫南\",\"燕青丝\"]}}]";
        DataStream<String> ds = env.addSource(new FieldGeneSouce(fieldGenesDescListNotNullEle, 2));
        DataStream<Map<String, Object>> rstDs = ds.map(new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        Properties info = new Properties();
        info.put("user", "default");
        info.put("password", "123456");
        MapBatchIntervalClickHouseSink clickHouseSink = new MapBatchIntervalClickHouseSink(10, 30 * 1000,
                "192.168.216.86:9001", "test.test_ck_sink_local", info);
        rstDs.addSink(clickHouseSink);

        /*rstDs.addSink(new SinkFunction<Map<String, Object>>() {
            @Override
            public void invoke(Map<String, Object> value, Context context) throws Exception {
                System.out.println(JSON.toJSONString(value));
            }
        });*/

        env.execute("MapBatchIntervalClickHouseSinkTest");
    }

}
