package com.java.flink.stream.theory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;

/**
 * env.getConfig.enableObjectReuse()配置
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/execution/execution_configuration/
 * https://blog.csdn.net/hellojoy/article/details/101460008
 * https://baijiahao.baidu.com/s?id=1707837423832621786&wfr=spider&for=pc
 * https://segmentfault.com/a/1190000022001684
 */
public class OperatorChainChainingOutputTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * ChainingOutput, 默认是CopyingChainingOutput
         * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/execution/execution_configuration/
         *
         * 看org.apache.flink.streaming.runtime.tasks.OperatorChain#createChainedSourceOutput：
         *   isObjectReuseEnabled()
         *      为true时，使用ChainingOutput
         *      为false时，使用CopyingChainingOutput(默认)
         *
         * ChainingOutput直接把record发送下游
         * CopyingChainingOutput需要serializer.copy(data)后发送下游，调用的是TypeSerializer.copy方法直接生成对象，task直接shuffle网络通信时调用的是TypeSerializer.serialize方法
         *
         * pojo对应的TypeSerializer是PojoSerializer
         */
        //env.getConfig().enableObjectReuse();

        DataStream<Data> ds = env.fromSequence(1, Integer.MAX_VALUE).map(x -> {
            Thread.sleep(1000);
           return new Data(x, x.toString());
        });

        ds.addSink(new RichSinkFunction<Data>() {
            @Override
            public void invoke(Data value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("OperatorChainChainingOutputTest");
    }

    public static class Data implements Serializable{
        private long id;
        private String name;

        public Data(long id, String name) {
            this.id = id;
            this.name = name;
            System.out.println("Data(id, name) init");
        }

        public Data() {
            System.out.println("Data() init");
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
