package com.java.flink.doris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DorisStringSinkTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("single:8030")
                .setTableIdentifier("test.object_statistics_a")
                .setUsername("root")
                .setPassword("");

        Properties properties = new Properties();
        properties.setProperty("format", "csv");
        properties.setProperty("columns", "vsys_id, object_id, timestamp_ms, item_id, device_id, in_bytes, out_bytes, bytes, new_in_sessions, new_out_sessions, sessions, datetime=from_unixtime(timestamp_ms/1000)");
        // 上游是json写入时，需要开启配置
        //properties.setProperty("format", "json");
        //properties.setProperty("read_json_by_line", "true");
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
                .setDeletable(false)
                .disable2PC() // 关闭两阶段提交, 否则必须从cp启动
                .setWriteMode(WriteMode.STREAM_LOAD_BATCH) // 是否使用攒批模式写入Doris，开启后写入时机不依赖Checkpoint，通过sink.buffer-flush.max-rows/sink.buffer-flush.max-bytes/sink.buffer-flush.interval 参数来控制写入时机。 同时开启后将不保证Exactly-once语义，可借助Uniq模型做到幂等
                .setMaxRetries(1)
                .setStreamLoadProp(properties); //streamload params

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(dorisBuilder.build());


        DataStreamSource<String> ds = env.fromElements(
                "1\t3\t1712732400000\t2\t1\t32978\t24920\t57898\t490\t35\t525",
                "1\t3\t1712732460000\t2\t1\t72503\t88724\t161227\t609\t818\t1427",
                "1\t3\t1712732520000\t2\t1\t19375\t39537\t58912\t55\t826\t881"
        );
        ds.sinkTo(builder.build());

        env.execute("DorisRowDataSinkTest");
    }

}
