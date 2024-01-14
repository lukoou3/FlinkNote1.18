package com.java.flink.stream.typeinfo;

import com.alibaba.fastjson2.JSON;
import com.java.flink.stream.func.FieldGeneSouce;
import com.java.flink.stream.func.LogMap;
import com.java.flink.stream.window.WindowAssignerTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class InputTypeConfigurableTest {
    String[] fieldGenesDesc = new String[]{
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"pageId\", \"start\":1, \"end\":3}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"userId\", \"start\":1, \"end\":5}}",
            "{\"type\":\"long_inc\", \"fields\":{\"name\":\"time\",\"start\":500, \"step\":1000}}",
            "{\"type\":\"int_random\", \"fields\":{\"name\":\"visitCnt\", \"start\":1, \"end\":1}}"
    };

    /**
     * 有的sink需要缓存元素，需要复制元素，可以实现InputTypeConfigurable接口，参考官方jdbcSink
     * org.apache.flink.streaming.api.datastream.DataStream#addSink方法中会调用：
     *
     *     public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {
     *
     *         // read the output type of the input Transform to coax out errors about MissingTypeInfo
     *         transformation.getOutputType();
     *
     *         // configure the type if needed
     *         if (sinkFunction instanceof InputTypeConfigurable) {
     *             ((InputTypeConfigurable) sinkFunction).setInputType(getType(), getExecutionConfig());
     *         }
     *
     *         return DataStreamSink.forSinkFunction(this, clean(sinkFunction));
     *     }
     */
    @Test
    public void sinkInputTypeConfigurableTest() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DataStream<String> ds = env.addSource(new FieldGeneSouce("[" + StringUtils.join(fieldGenesDesc, ",") + "]", 1, 1000));

        ds.map(x -> JSON.parseObject(x, OnlineLog.class))
                .map(new LogMap<>())
                .addSink(new SinkFuncInputTypeConfigurable());

        env.execute();
    }

    public static class SinkFuncInputTypeConfigurable extends RichSinkFunction<OnlineLog> implements InputTypeConfigurable {
        @Nullable
        private TypeSerializer<OnlineLog> serializer;

        @Override
        public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
            if (executionConfig.isObjectReuseEnabled()) {
                this.serializer = (TypeSerializer<OnlineLog>) type.createSerializer(executionConfig);
            }
        }

        @Override
        public void invoke(OnlineLog record, Context context) throws Exception {
            // copyIfNecessary
            OnlineLog copy =  serializer == null ? record : serializer.copy(record);
            System.out.println(copy);
        }
    }

    public static class OnlineLog {
        public int pageId;
        public int userId;
        public long time;
        public int visitCnt;

        public WindowAssignerTest.OnlineLog copy() {
            WindowAssignerTest.OnlineLog copy = new WindowAssignerTest.OnlineLog();
            copy.pageId = pageId;
            copy.userId = userId;
            copy.time = time;
            copy.visitCnt = visitCnt;
            return copy;
        }

        @Override
        public String toString() {
            return "OnlineLog{" +
                    "pageId=" + pageId +
                    ", userId=" + userId +
                    ", time=" + time +
                    ", visitCnt=" + visitCnt +
                    '}';
        }
    }
}
