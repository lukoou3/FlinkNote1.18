package com.java.flink.doris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class DorisRowDataSinkTest {

    public static void main(String[] args) throws Exception {
        // testArrow(); // 可以
        // testArrow2(); // 不行, arrow不能用函数转换列
        // testCsv(); // 可以
        testCsv2(); //关闭2pc, 不从cp启动
    }

    public static void testArrow2() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        //doris sink option
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("single:8030")
                .setTableIdentifier("test.object_statistics_a")
                .setUsername("root")
                .setPassword("");

        // csv format to streamload
        Properties properties = new Properties();
        properties.setProperty("format", "arrow");
        properties.setProperty("columns", "vsys_id, object_id, timestamp, item_id, device_id, in_bytes, out_bytes, bytes, new_in_sessions, new_out_sessions, sessions, datetime=from_unixtime(timestamp)");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris3") //streamload label prefix
                .setDeletable(false)
                .setStreamLoadProp(properties); //streamload params

        //flink rowdata‘s schema
        String[] fields = {"vsys_id", "object_id", "timestamp", "item_id", "device_id", "in_bytes", "out_bytes", "bytes", "new_in_sessions", "new_out_sessions", "sessions"};
        DataType[] types = {DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.STRING(),
                DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()};

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("arrow")           //csv format
                        .setFieldType(types).build())
                .setDorisOptions(dorisBuilder.build());

        GenericRowData[] datas = new GenericRowData[]{new GenericRowData(11), new GenericRowData(11), new GenericRowData(11)};
        GenericRowData data;
        for (int i = 0; i < datas.length; i++) {
            data = datas[i];
            data.setField(0, 1L);
            data.setField(1, 5L);
            data.setField(2, 1712728800L + i);
            data.setField(3, 2L);
            data.setField(4, StringData.fromString("3"));
            for (int j = 5; j < 11; j++) {
                data.setField(j, (long)ThreadLocalRandom.current().nextInt(10 * j));
            }
        }

        DataStream<RowData> source = env.fromElements(datas);
        source.sinkTo(builder.build());

        env.execute("DorisRowDataSinkTest");
    }

    public static void testArrow() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        //doris sink option
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("single:8030")
                .setTableIdentifier("test.object_statistics_a")
                .setUsername("root")
                .setPassword("");

        // csv format to streamload
        Properties properties = new Properties();
        properties.setProperty("format", "arrow");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris2") //streamload label prefix
                .setDeletable(false)
                .setStreamLoadProp(properties); //streamload params

        //flink rowdata‘s schema
        String[] fields = {"vsys_id", "object_id", "datetime", "item_id", "device_id", "in_bytes", "out_bytes", "bytes", "new_in_sessions", "new_out_sessions", "sessions"};
        DataType[] types = {DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.TIMESTAMP(0), DataTypes.BIGINT(), DataTypes.STRING(),
                DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()};

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("arrow")           //csv format
                        .setFieldType(types).build())
                .setDorisOptions(dorisBuilder.build());

        GenericRowData[] datas = new GenericRowData[]{new GenericRowData(11), new GenericRowData(11), new GenericRowData(11)};
        GenericRowData data;
        for (int i = 0; i < datas.length; i++) {
            data = datas[i];
            data.setField(0, 1L);
            data.setField(1, 5L);
            data.setField(2, TimestampData.fromEpochMillis((1712728800L + i) * 1000));
            data.setField(3, 2L);
            data.setField(4, StringData.fromString("3"));
            for (int j = 5; j < 11; j++) {
                data.setField(j, (long)ThreadLocalRandom.current().nextInt(10 * j));
            }
        }

        DataStream<RowData> source = env.fromElements(datas);
        source.sinkTo(builder.build());

        env.execute("DorisRowDataSinkTest");
    }

    public static void testCsv2() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        //doris sink option
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("single:8030")
                .setTableIdentifier("test.object_statistics_a")
                .setUsername("root")
                .setPassword("");

        // csv format to streamload
        Properties properties = new Properties();
        properties.setProperty("format", "csv");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
                .setDeletable(false)
                .disable2PC() // 关闭两阶段提交, 否则必须从cp启动
                .setWriteMode(WriteMode.STREAM_LOAD_BATCH) // 是否使用攒批模式写入Doris，开启后写入时机不依赖Checkpoint，通过sink.buffer-flush.max-rows/sink.buffer-flush.max-bytes/sink.buffer-flush.interval 参数来控制写入时机。 同时开启后将不保证Exactly-once语义，可借助Uniq模型做到幂等
                .setMaxRetries(1)
                .setStreamLoadProp(properties); //streamload params

        //flink rowdata‘s schema
        String[] fields = {"vsys_id", "object_id", "datetime", "item_id", "device_id", "in_bytes", "out_bytes", "bytes", "new_in_sessions", "new_out_sessions", "sessions"};
        DataType[] types = {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.INT(),
                DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT()};

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("csv")           //csv format
                        .setFieldDelimiter("\t")
                        .setFieldType(types).build())
                .setDorisOptions(dorisBuilder.build());

        GenericRowData[] datas = new GenericRowData[]{new GenericRowData(11), new GenericRowData(11), new GenericRowData(11)};
        GenericRowData data;
        for (int i = 0; i < datas.length; i++) {
            data = datas[i];
            data.setField(0, 1);
            data.setField(1, 5);
            data.setField(2, StringData.fromString("2024-04-10 13:00:0" + i % 10));
            data.setField(3, 2);
            data.setField(4, 3);
            for (int j = 5; j < 11; j++) {
                data.setField(j, ThreadLocalRandom.current().nextInt(10 * j));
            }
        }

        DataStream<RowData> source = env.fromElements(datas);
        source.sinkTo(builder.build());

        env.execute("DorisRowDataSinkTest");
    }

    public static void testCsv() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        //doris sink option
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("single:8030")
                .setTableIdentifier("test.object_statistics_a")
                .setUsername("root")
                .setPassword("");

        // csv format to streamload
        Properties properties = new Properties();
        properties.setProperty("format", "csv");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
                .setDeletable(false)
                .setStreamLoadProp(properties); //streamload params

        //flink rowdata‘s schema
        String[] fields = {"vsys_id", "object_id", "datetime", "item_id", "device_id", "in_bytes", "out_bytes", "bytes", "new_in_sessions", "new_out_sessions", "sessions"};
        DataType[] types = {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.INT(),
                DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT()};

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("csv")           //csv format
                        .setFieldDelimiter("\t")
                        .setFieldType(types).build())
                .setDorisOptions(dorisBuilder.build());

        GenericRowData[] datas = new GenericRowData[]{new GenericRowData(11), new GenericRowData(11), new GenericRowData(11)};
        GenericRowData data;
        for (int i = 0; i < datas.length; i++) {
            data = datas[i];
            data.setField(0, 1);
            data.setField(1, 5);
            data.setField(2, StringData.fromString("2024-04-10 12:00:0" + i % 10));
            data.setField(3, 2);
            data.setField(4, 3);
            for (int j = 5; j < 11; j++) {
                data.setField(j, ThreadLocalRandom.current().nextInt(10 * j));
            }
        }

        DataStream<RowData> source = env.fromElements(datas);
        source.sinkTo(builder.build());

        env.execute("DorisRowDataSinkTest");
    }
}
