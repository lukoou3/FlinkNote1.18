package com.java.flink.connector.test;

import com.alibaba.fastjson2.JSON;
import com.clickhouse.client.data.ClickHouseBitmap;
import com.java.flink.connector.clickhouse.sink.MapBatchIntervalClickHouseSink;
import com.java.flink.stream.func.FieldGeneSouce;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.*;

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

    /**
     * AbstractBatchIntervalClickHouseSink 优化gc的主要考虑是：
     *     如果使用batch list 缓存，gc压力会比较大
     *     所以考虑把batch写入buffer，buffer复用。减少gc。考虑到需要进行失败重试，所以把Block缓存。使用反射替换Block内容。
     *     Block里面的列也就是IColumn类对象，IColumn的是从缓存池中申请的ColumnWriterBuffer buffer，会归还。归还就会复用。不归还ColumnWriterBuffer buffer就会被gc释放，不影响，不会内存泄漏。
     *     缓冲池使用的是ConcurrentLinkedDeque<ColumnWriterBuffer>，放入才会缓存，申请会从队列删除，不归还不会内存泄漏，申请没有限制，队列中没有对象会申请创建。
     *     ck sink close函数中缓存的Block不用归还释放列IColumn申请的ColumnWriterBuffer，会被gc。
     *     ConcurrentLinkedDeque<ColumnWriterBuffer> stack 缓存池没有记录列表总大小，使用大小等信息，没限制列表大小。不归还ColumnWriterBuffer没问题。
     *
     */
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

    @Test
    public void testError() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        ClickHouseBitmap bitmap = ClickHouseBitmap.wrap(1, 2, 3, 4, 5);
        List<Map<String, Object>> datas = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("datetime", 1614566501);
        data.put("1614566500", "a");
        data.put("value", bitmap.toBytes());
        datas.add(data);
        data = new HashMap<>();
        data.put("datetime", 1614566501);
        data.put("1614566500", "a");
        data.put("value", bitmap.toBytes());
        datas.add(data);
        DataStream<Map<String, Object>> rstDs =env.fromCollection(datas);

        Properties info = new Properties();
        info.put("user", "default");
        info.put("password", "123456");
        MapBatchIntervalClickHouseSink clickHouseSink = new MapBatchIntervalClickHouseSink(10, 30 * 1000,
                "192.168.216.86:9001", "test.test_bitmap", info);
        rstDs.addSink(clickHouseSink);

        env.execute("MapBatchIntervalClickHouseSinkTest");
    }

    @Test
    public void testBitmap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        ClickHouseBitmap bitmap1 = ClickHouseBitmap.wrap(1, 2, 3, 4, 5);
        ClickHouseBitmap bitmap2 = ClickHouseBitmap.wrap(4, 5, 8, 9, 10);
        List<Map<String, Object>> datas = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("datetime", 1614566501);
        data.put("name", "a");
        data.put("roaring_str", bitmap1.toBytes());
        datas.add(data);
        data = new HashMap<>();
        data.put("datetime", 1614566501);
        data.put("name", "b");
        data.put("roaring_str", bitmap2.toBytes());
        datas.add(data);
        DataStream<Map<String, Object>> rstDs =env.fromCollection(datas);

        Properties info = new Properties();
        info.put("user", "default");
        info.put("password", "123456");
        MapBatchIntervalClickHouseSink clickHouseSink = new MapBatchIntervalClickHouseSink(10, 30 * 1000,
                "192.168.216.86:9001", "test.test_bitmap2", info);
        rstDs.addSink(clickHouseSink);

        env.execute("MapBatchIntervalClickHouseSinkTest");
    }

    @Test
    public void testBitmap2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        Random random = new Random();

        List<Map<String, Object>> datas = new ArrayList<>();
        int ts = 1614566501 + 86400 + 86400;
        for (int i = 0; i < 80000; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("datetime", ts);
            data.put("name", "" + i % 5);
            ClickHouseBitmap bitmap = ClickHouseBitmap.wrap(random.nextInt(400), random.nextInt(400), random.nextInt(400), random.nextInt(400), 5);
            data.put("roaring_str", bitmap.toBytes());
            datas.add(data);
        }

        DataStream<Map<String, Object>> rstDs =env.fromCollection(datas);

        Properties info = new Properties();
        info.put("user", "default");
        info.put("password", "123456");
        MapBatchIntervalClickHouseSink clickHouseSink = new MapBatchIntervalClickHouseSink(100000, 30 * 1000,
                "192.168.216.86:9001", "test.test_bitmap2", info);
        rstDs.addSink(clickHouseSink);

        env.execute("MapBatchIntervalClickHouseSinkTest");
    }
}
