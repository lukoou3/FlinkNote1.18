package com.java.flink.stream.high.config;

import com.java.flink.base.FlinkBaseTest;
import com.java.flink.stream.func.UniqueIdSequenceSource;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

/**
 * execution.buffer-timeout.enabled：是否启用execution.buffer-timeout.interval配置
 *     默认是true
 *     如果设置为false，只有当输出缓冲区已满时才会触发刷新，从而最大化吞吐量
 * execution.buffer-timeout.interval: output buffer最大输出间隔。
 *     默认是100ms
 *     设置为0，则每条数据会立马发出。延迟低，但是吞吐量小。
 * 这个配置一般不需要调，除非对延时有要求，100ms已经很低了。
 */
public class BufferTimeoutTest extends FlinkBaseTest {

    // 默认相差107ms
    @Test
    public void testBufferTimeoutDefault() throws Exception {
        initEnv();

        env.addSource(new UniqueIdSequenceSource(1000))
                .map(id -> System.currentTimeMillis())
                .disableChaining()
                .addSink(new SinkFunction<Long>() {
                    @Override
                    public void invoke(Long value, Context context) throws Exception {
                        long ts = System.currentTimeMillis();
                        System.out.println(new java.sql.Timestamp(ts) + ":" + (ts - value));
                    }
                });

        env.execute();
    }

    /**
     * 配置 => 时间差
     * 10 => 16
     * 20 => 31
     * 50 => 61
     * 100 => 107
     * 200 => 210
     */
    @Test
    public void testBufferTimeoutSet() throws Exception {
        initEnv(conf -> {
        }, env -> env.setBufferTimeout(200));

        env.addSource(new UniqueIdSequenceSource(1000))
                .map(id -> System.currentTimeMillis())
                .disableChaining()
                .addSink(new SinkFunction<Long>() {
                    @Override
                    public void invoke(Long value, Context context) throws Exception {
                        long ts = System.currentTimeMillis();
                        System.out.println(new java.sql.Timestamp(ts) + ":" + (ts - value));
                    }
                });

        env.execute();
    }
}
