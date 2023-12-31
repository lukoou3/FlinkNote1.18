package com.java.flink.stream.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.io.Serializable;
import java.util.Optional;

import com.java.flink.stream.join.Union2JoinKeyedProcessFunction.Union2Data;
import org.apache.flink.util.Collector;

public class Union2JoinKeyedProcessFunction<K, D1, D2> extends KeyedProcessFunction<K, Union2Data<K, D1, D2>, Union2Data<K, D1, D2>> {
    final private long delayMs;
    final private boolean objectReuse;
    final private StateTtlConfig ttlConfig1;
    final private StateTtlConfig ttlConfig2;
    final private ReplaceAndHandleSetter<D1> replaceAndHandleSetter1;
    final private ReplaceAndHandleSetter<D2> replaceAndHandleSetter2;
    final private TypeInformation<D1> typeInfo1;
    final private TypeInformation<D2> typeInfo2;
    final private ReplaceAndHandleTip tip;
    private Union2Data<K, D1, D2> outData;
    private ValueState<D1> data1State;
    private ValueState<D2> data2State;
    private ValueState<Long> timeState;


    public Union2JoinKeyedProcessFunction(long delayMs, boolean objectReuse, Optional<StateTtlConfig> ttlConfig1, Optional<StateTtlConfig> ttlConfig2,
                                          Optional<ReplaceAndHandleSetter<D1>> replaceAndHandleSetter1, Optional<ReplaceAndHandleSetter<D2>> replaceAndHandleSetter2,
                                          TypeInformation<D1> typeInfo1, TypeInformation<D2> typeInfo2) {
        this.delayMs = delayMs;
        this.objectReuse = objectReuse;
        this.ttlConfig1 = ttlConfig1.orElse(null);
        this.ttlConfig2 = ttlConfig2.orElse(null);
        this.replaceAndHandleSetter1 = replaceAndHandleSetter1.orElse(defaultReplaceAndHandleSetter());
        this.replaceAndHandleSetter2 = replaceAndHandleSetter2.orElse(defaultReplaceAndHandleSetter());
        this.typeInfo1 = typeInfo1;
        this.typeInfo2 = typeInfo2;
        this.tip = new ReplaceAndHandleTip();
        if (this.objectReuse) {
            this.outData = new Union2Data<K, D1, D2>();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<D1> data1StateDescriptor = new ValueStateDescriptor<>("data1-state", typeInfo1);
        ValueStateDescriptor<D2> data2StateDescriptor = new ValueStateDescriptor<>("data2-state", typeInfo2);
        if (ttlConfig1 != null) {
            data1StateDescriptor.enableTimeToLive(ttlConfig1);
        }
        if (ttlConfig2 != null) {
            data2StateDescriptor.enableTimeToLive(ttlConfig2);
        }
        data1State = getRuntimeContext().getState(data1StateDescriptor);
        data2State = getRuntimeContext().getState(data2StateDescriptor);
        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", BasicTypeInfo.LONG_TYPE_INFO));
    }

    @Override
    public void processElement(Union2Data<K, D1, D2> value, KeyedProcessFunction<K, Union2Data<K, D1, D2>, Union2Data<K, D1, D2>>.Context ctx, Collector<Union2Data<K, D1, D2>> out) throws Exception {
        if (value.data1 != null) {
            processElement1(value, ctx, out);
        } else if (value.data2 != null) { // 这个必须用else if,processElement1可能为data2赋值,这样写没问题:data1和data2不可能同时不为null
            processElement2(value, ctx, out);
        }
    }

    void processElement1(Union2Data<K, D1, D2> value, KeyedProcessFunction<K, Union2Data<K, D1, D2>, Union2Data<K, D1, D2>>.Context ctx, Collector<Union2Data<K, D1, D2>> out) throws Exception {
        D1 data1 = value.data1;
        D1 hisData1 = data1State.value();
        tip.reSetReplaceAndHandleTrue();
        if (hisData1 != null) {
            replaceAndHandleSetter1.set(data1, hisData1, tip);
        }
        if (tip.replace) {
            data1State.update(data1);
        }
        if (!tip.replace || !tip.handle) {
            return;
        }

        D2 data2 = data2State.value();
        if (data2 != null) {
            value.data2 = data2;
            out.collect(value);

            if (timeState.value() != null) {
                ctx.timerService().deleteProcessingTimeTimer(timeState.value());
                timeState.clear();
            }
        } else if (timeState.value() == null) {
            long time = ctx.timerService().currentProcessingTime() + delayMs;
            ctx.timerService().registerProcessingTimeTimer(time);
            timeState.update(time);
        }
    }

    void processElement2(Union2Data<K, D1, D2> value, KeyedProcessFunction<K, Union2Data<K, D1, D2>, Union2Data<K, D1, D2>>.Context ctx, Collector<Union2Data<K, D1, D2>> out) throws Exception {
        D2 data2 = value.data2;
        D2 hisData2 = data2State.value();
        tip.reSetReplaceAndHandleTrue();
        if (hisData2 != null) {
            replaceAndHandleSetter2.set(data2, hisData2, tip);
        }
        if (tip.replace) {
            data2State.update(data2);
        }
        if (!tip.replace || !tip.handle) {
            return;
        }

        D1 data1 = data1State.value();
        if (data1 != null) {
            value.data1 = data1;
            out.collect(value);

            if (timeState.value() != null) {
                ctx.timerService().deleteProcessingTimeTimer(timeState.value());
                timeState.clear();
            }
        } else if (timeState.value() == null) {
            long time = ctx.timerService().currentProcessingTime() + delayMs;
            ctx.timerService().registerProcessingTimeTimer(time);
            timeState.update(time);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<K, Union2Data<K, D1, D2>, Union2Data<K, D1, D2>>.OnTimerContext ctx, Collector<Union2Data<K, D1, D2>> out) throws Exception {
        timeState.clear();
        D1 data1 = data1State.value();
        D2 data2 = data2State.value();
        if (data1 != null && data2 != null) {

        } else {
            if (!objectReuse) {
                outData = new Union2Data<>();
            }

            K key = ctx.getCurrentKey();
            outData.setKey(key);
            if (data1 == null) {
                data1 = tryQueryElement1(key);
                if (data1 != null) {
                    data1State.update(data1);
                }
            }
            outData.setData1(data1);
            if (data2 == null) {
                data2 = tryQueryElement2(key);
                if (data2 != null) {
                    data2State.update(data2);
                }
            }
            outData.setData2(data2);
            out.collect(outData);
        }
    }

    D1 tryQueryElement1(K key) {
        return null;
    }

    D2 tryQueryElement2(K key) {
        return null;
    }

    static <T> ReplaceAndHandleSetter<T> defaultReplaceAndHandleSetter() {
        return new ReplaceAndHandleSetter<T>() {
            @Override
            public void set(T newData, T oldData, ReplaceAndHandleTip tip) {
            }
        };
    }

    public static <K, D1, D2> KeyedStream<Union2Data<K, D1, D2>, K> union(DataStream<D1> ds1, DataStream<D2> ds2, KeySelector<D1, K> key1, KeySelector<D2, K> key2, TypeInformation<Union2Data<K, D1, D2>> typeInfo) {
        return ds1.map(new MapFunction<D1, Union2Data<K, D1, D2>>()  {
            Union2Data data = new Union2Data<K, D1, D2>();

            @Override
            public Union2Data<K, D1, D2> map(D1 value) throws Exception {
                data.key = key1.getKey(value);
                data.data1 = value;
                return data;
            }
        }).returns(typeInfo).setParallelism(ds1.getParallelism()).union(
                ds2.map(new MapFunction<D2, Union2Data<K, D1, D2>>() {
                            Union2Data data = new Union2Data<K, D1, D2>();

                            @Override
                            public Union2Data<K, D1, D2> map(D2 value) throws Exception {
                                data.key = key2.getKey(value);
                                data.data2 = value;
                                return data;
                            }
                        }
                ).returns(typeInfo).setParallelism(ds2.getParallelism())
        ).keyBy(x -> x.key);
    }

    public static class ReplaceAndHandleTip implements Serializable {
        boolean replace;
        boolean handle;

        public void setReplaceAndHandle(boolean replace, boolean handle) {
            this.replace = replace;
            this.handle = handle;
        }

        public void reSetReplaceAndHandleFalse() {
            this.replace = false;
            this.handle = false;
        }

        public void reSetReplaceAndHandleTrue() {
            this.replace = true;
            this.handle = true;
        }
    }

    public static class Union2Data<K, D1, D2> implements Serializable {
        K key;
        D1 data1;
        D2 data2;

        public K getKey() {
            return key;
        }

        public void setKey(K key) {
            this.key = key;
        }

        public D1 getData1() {
            return data1;
        }

        public void setData1(D1 data1) {
            this.data1 = data1;
        }

        public D2 getData2() {
            return data2;
        }

        public void setData2(D2 data2) {
            this.data2 = data2;
        }
    }

    @FunctionalInterface
    public interface ReplaceAndHandleSetter<D> extends Serializable{
        void set(D newData, D oldData, ReplaceAndHandleTip tip);
    }
}
