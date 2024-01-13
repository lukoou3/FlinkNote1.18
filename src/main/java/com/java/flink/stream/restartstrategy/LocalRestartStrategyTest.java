package com.java.flink.stream.restartstrategy;

import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

/**
 * taskexecutor运行task抛出异常：
 * org.apache.flink.runtime.taskmanager.Task#restoreAndInvoke 发现异常，close task后throw throwable
 * org.apache.flink.runtime.taskmanager.Task#doRun() 捕获异常，在finally中释放资源：Freeing task resources
 * org.apache.flink.runtime.taskmanager.Task#notifyFinalState() 向jobmanager发送任务状态，这个是失败
 * org.apache.flink.runtime.taskexecutor.TaskExecutor.TaskManagerActionsImpl#updateTaskExecutionState
 * org.apache.flink.runtime.taskexecutor.TaskExecutor#unregisterTaskAndNotifyFinalState
 * org.apache.flink.runtime.taskexecutor.TaskExecutor#updateTaskExecutionState
 * org.apache.flink.runtime.jobmaster.JobMasterGateway#updateTaskExecutionState // 发送网路请求
 *
 * jobmanager收到任务失败：
 * org.apache.flink.runtime.jobmaster.JobMaster.updateTaskExecutionState // 收到请求
 * org.apache.flink.runtime.scheduler.DefaultScheduler.restartTasksWithDelay
 * org.apache.flink.runtime.scheduler.DefaultExecutionDeployer.waitForAllSlotsAndDeploy
 * org.apache.flink.runtime.executiongraph.Execution.deploy
 * org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway.submitTask // 发送请求
 *
 * taskexecutor收到任务部署：
 * org.apache.flink.runtime.taskexecutor.TaskExecutor#submitTask // 收到请求
 * org.apache.flink.runtime.taskmanager.Task#startTaskThread
 * org.apache.flink.runtime.taskmanager.Task#run
 * org.apache.flink.runtime.taskmanager.Task#restoreAndInvoke(invokable) //invokable是反序列化的SourceStreamTask(StreamTask)
 * org.apache.flink.streaming.runtime.tasks.StreamTask#invoke
 */
public class LocalRestartStrategyTest {
    static AtomicInteger retry = new AtomicInteger();
    static final Logger LOG = LoggerFactory.getLogger(LocalRestartStrategyTest.class);
    static String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":0, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    /**
     * 没有配置cp的时候，默认是没有重启策略的，显示配置才行，程序里配置和配置文件配置都行
     * local、集群、yarn提交都是这样。这里只是使用local模式便与调试
     * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/task_failure_recovery/
     * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/ops/state/task_failure_recovery/
     * restart-strategy变成restart-strategy.type了
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 没有配置cp的时候，默认是没有重启策略的，显示配置才行，程序里配置和配置文件配置都行
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(5, TimeUnit.SECONDS) // delay
        ));

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                LOG.warn("open");
            }

            @Override
            public String map(String value) throws Exception {
                if(getRuntimeContext().getIndexOfThisSubtask() == 1){
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
        }).addSink(new LogSink<>());

        env.execute();
    }

}
