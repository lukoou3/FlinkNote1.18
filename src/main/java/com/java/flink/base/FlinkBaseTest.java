package com.java.flink.base;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;

import java.util.function.Consumer;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public abstract class FlinkBaseTest {
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    public void initEnv() throws Exception {
        initEnv(conf -> {}, env -> {});
    }

    public void initEnv(Consumer<Configuration> initConfiguration, Consumer<StreamExecutionEnvironment> initEnv) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        initConfiguration.accept(conf);
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        initEnv.accept(env);

        tEnv = StreamTableEnvironment.create(env) ;
    }

}
