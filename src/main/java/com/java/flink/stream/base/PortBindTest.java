package com.java.flink.stream.base;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.shaded.guava31.com.google.common.base.Preconditions.checkArgument;

/**
 * 通过配置rest.bind-port，可以支持端口被占用时使用其它端口尝试
 * rest.bind-port: 8081-8085
 * rest.bind-port: 8081,8082,8083
 * 通过测试，StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)和new MiniCluster(miniClusterConfig)都可以实现跳过占用的端口
 * 测试这个是因为在jupyter中启动flink时，默认的配置文件是单个端口，只能同时运行一个notebook，配置多端口可以同时运行多个notebook
 */
public class PortBindTest {

    public static void main(String[] args) throws Exception {
        checkArgument(args.length > 0, "args can not is empty");
        if(args[0].equals("1")){
            normalStartup();
        } else if (args[0].equals("2")) {
            manyPortStartup();
        } else if (args[0].equals("3")) {
            manyPortStartupMiniCluster();
        }
    }

    public static void normalStartup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> text = env.fromElements("1", "2", "3");
        text.map(ele -> {
            Thread.sleep(1000 * 60 * 5);
            return ele;
        }).print();

        env.execute("PortBindTest");
    }

    public static void manyPortStartup() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStream<String> text = env.fromElements("1", "2", "3");
        text.map(ele -> {
            Thread.sleep(1000 * 60 * 5);
            return ele;
        }).print();

        env.execute("PortBindTest");
    }

    public static void manyPortStartupMiniCluster() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        MiniClusterConfiguration miniClusterConfig = new MiniClusterConfiguration.Builder()
                .setConfiguration(conf)
                .setNumSlotsPerTaskManager(1)
                .setNumTaskManagers(1)
                .build();

        MiniCluster cluster = new MiniCluster(miniClusterConfig);
        cluster.start();

        int port = cluster.getRestAddress().get().getPort();
        System.out.println(cluster.getRestAddress().get());
        System.out.println(port);

        Thread.sleep(1000 * 60 * 5);
    }
}
