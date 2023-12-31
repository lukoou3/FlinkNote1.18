package com.java.flink.stream.func;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedIntervalSendProcessFunction<K, T> extends KeyedProcessFunction<K, T, T> implements ResultTypeQueryable {
    final private long intervalMs;
    final private boolean eagerMode;
    final private TypeInformation<T> typeInfo;

    private ValueState<T> dataState; // 数据
    private ValueState<Long> timeState; // 设置的定时器时间
    private long firstDelayTs = 0L; //首次延时未发送元素的时间，用于验证程序逻辑正确性

    /**
     * 每个元素最多每隔intervalMs输出一次最新的数据
     *
     * @param intervalMs 周期，单位毫秒
     * @param eagerMode 是否急切模式
     *                   true：元素第一次来时就立即发送，之后过了interval后才会再次发送最新的元素。
     *                      实现相对复杂，使用第二种实现简单的方案：
     *                          1、元素第一次来时发送并记录时间，元素再来时判断是否发送还是定时发送
     *                          2、来了元素看是否有定时器，没定时器就立刻发送设置定时，有定时器就更新元素，等定时到了发送元素(期间有新的元素来)并清空定时
     *                   false: 元素第一次来时过interval后发送最新的元素，interval清除后重复此步骤。
     *                      实现简单：来了元素判断是否定时就行
     * @param typeInfo T类型元素的TypeInformation
     */
    public KeyedIntervalSendProcessFunction(long intervalMs, boolean eagerMode, TypeInformation<T> typeInfo) {
        this.intervalMs = intervalMs;
        this.eagerMode = eagerMode;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataState = getRuntimeContext().getState(new ValueStateDescriptor<T>("data-state", typeInfo));
        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", BasicTypeInfo.LONG_TYPE_INFO));
    }

    // 是否是新的元素，旧的元素直接忽略
    boolean isNewData (T newData, T oldData){
        return true;
    }

    @Override
    public void processElement(T data, KeyedProcessFunction<K, T, T>.Context ctx, Collector<T> out) throws Exception {
        T preData = dataState.value();
        if (eagerMode) {
            if (timeState.value() == null) {
                out.collect(data);
                // 下次允许发送元素的最早时间
                long time = ctx.timerService().currentProcessingTime() + intervalMs;
                ctx.timerService().registerProcessingTimeTimer(time);
                timeState.update(time);
            } else {
                if (preData == null || isNewData(data, preData)) {
                    verifyDelay();
                    dataState.update(data);
                }
            }
        } else {
            if (preData == null || isNewData(data, preData)) {
                dataState.update(data);

                // 设置过了就不用再设置, 等输出后timeState会清空需要重新设置
                if (timeState.value() == null) {
                    // 发送元素的时间
                    long time = ctx.timerService().currentProcessingTime() + intervalMs;
                    ctx.timerService().registerProcessingTimeTimer(time);
                    timeState.update(time);
                } else {
                    verifyDelay();
                }
            }
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<K, T, T>.OnTimerContext ctx, Collector<T> out) throws Exception {
        T data = dataState.value();

        if (eagerMode) {
            if (data != null) {
                out.collect(data);
            }
        } else {
            out.collect(data);
        }

        dataState.clear();
        timeState.clear();

        firstDelayTs = 0L;
    }

    private void verifyDelay(){
        if (firstDelayTs <= 0L) {
            firstDelayTs = System.currentTimeMillis();
        } else {
            if (System.currentTimeMillis() - firstDelayTs > intervalMs * 2) {
                throw new RuntimeException("逻辑错误或者定时器延时严重");
            }
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return typeInfo;
    }
}
