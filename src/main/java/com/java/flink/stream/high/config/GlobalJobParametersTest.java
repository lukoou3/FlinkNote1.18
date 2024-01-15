package com.java.flink.stream.high.config;

import com.java.flink.base.FlinkBaseTest;
import com.java.flink.stream.func.UniqueIdSequenceSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.LongValue;
import org.junit.Test;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;


/**
 * env.getConfig()返回的ExecutionConfig会被序列化到taskManager
 */
public class GlobalJobParametersTest extends FlinkBaseTest {

    @Test
    public void testGlobalJobParameters() throws Exception {
        initEnv(conf -> {
        }, env -> {
            Configuration config = new Configuration();
            config.setString("a", "b");
            config.set(HEARTBEAT_TIMEOUT, 300L);
            env.getConfig().setGlobalJobParameters(config);
        });

        env.addSource(new UniqueIdSequenceSource(1000))
                .map(new RichMapFunction<LongValue, String>() {
                    @Override
                    public String map(LongValue value) throws Exception {
                        ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
                        ExecutionConfig.GlobalJobParameters globalJobParameters = executionConfig.getGlobalJobParameters();
                        Configuration config = (Configuration) globalJobParameters;
                        System.out.println(config.getString("a", null));
                        System.out.println(config.get(HEARTBEAT_TIMEOUT));
                        return globalJobParameters.getClass() + ";" + globalJobParameters;
                    }
                })
                .print();

        env.execute();
    }

}
