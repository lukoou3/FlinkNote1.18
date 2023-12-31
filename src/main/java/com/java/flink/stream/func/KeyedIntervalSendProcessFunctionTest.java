package com.java.flink.stream.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

/**
 * 代码和效果和1.12一样，区别是StateBackend设置api变了(小问题)
 *
 * 经过测试：
 *    isEager = true正常，状态和定时器也能恢复
 *    isEager = false正常，状态和定时器也能恢复
 */
public class KeyedIntervalSendProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length > 0) {
            String checkpointPath = args[0];
            conf.setString("execution.savepoint.path", checkpointPath);
            System.out.println(checkpointPath);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.getConfig().enableObjectReuse();

        env.enableCheckpointing(1000 * 10);
        /**
         * 和1.12这样设置效果一样, 可以看FsStateBackend类的注释
         * env.setStateBackend(new FsStateBackend("file:///F:/flink-checkpoints"))
         */
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/flink-checkpoints");
        // 表示下 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        final long startTs = System.currentTimeMillis();

        SingleOutputStreamOperator<Tuple2<String, Long>> words = text.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                for (String word : value.trim().split("\\s+")) {
                    if (!word.isEmpty()) {
                        out.collect(Tuple2.of(word, System.currentTimeMillis() - startTs));
                    }
                }
            }
        });

        words.keyBy(x -> x.f0)
                .process(new KeyedIntervalSendProcessFunction<String, Tuple2<String, Long>>(1000 * 10, false, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .addSink(new RichSinkFunction<Tuple2<String, Long>>() {
                    @Override
                    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                        long ts = System.currentTimeMillis() - startTs;
                        System.out.println(String.format("out:%d: %d: %s", ts, ts - value.f1, value));
                    }
                });

        env.execute("KeyedIntervalSendProcessFunctionTest");
    }

}
