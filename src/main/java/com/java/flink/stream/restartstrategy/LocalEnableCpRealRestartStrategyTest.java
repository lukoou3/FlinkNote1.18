package com.java.flink.stream.restartstrategy;

import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

/**
 * enableCheckpointing时，默认是重启策略是fixed-delay strategy，重试次数：Integer.MAX_VALUE
 * local、集群、yarn提交都是这样。这里只是使用local模式便与调试
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/task_failure_recovery/
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/ops/state/task_failure_recovery/
 */
public class LocalEnableCpRealRestartStrategyTest {
    static AtomicInteger retry = new AtomicInteger();
    static final Logger LOG = LoggerFactory.getLogger(LocalEnableCpRealRestartStrategyTest.class);
    static String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":0, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        long startTs = System.currentTimeMillis();
        env.enableCheckpointing(10000);

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(new MyRichMapFunction(startTs)).addSink(new LogSink<>());

        env.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<String, String> implements CheckpointedFunction{
        final long startTs;

        public MyRichMapFunction(long startTs) {
            this.startTs = startTs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            LOG.warn("open");
        }

        @Override
        public String map(String value) throws Exception {
            if(getRuntimeContext().getIndexOfThisSubtask() == 1 && System.currentTimeMillis() - startTs > 15000){
                if(ThreadLocalRandom.current().nextInt(6) == 5){
                    int i = retry.incrementAndGet();
                    throw new RuntimeException("模拟异常:" + i);
                }
            }
            return value;
        }

        @Override
        public void close() throws Exception {
            LOG.warn("close");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            OperatorStateStore stateStore = context.getOperatorStateStore();
            ListState<Integer> offsets = stateStore.getUnionListState(new ListStateDescriptor<>("offsets", TypeInformation.of(Integer.class)));
            LOG.warn("offsets:{}", offsets);
            if(!offsets.get().iterator().hasNext()){
                offsets.add(1);
                offsets.add(2);
                offsets.add(3);
            }
        }
    }
}
