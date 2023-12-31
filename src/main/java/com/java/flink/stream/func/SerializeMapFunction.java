package com.java.flink.stream.func;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;

public class SerializeMapFunction<IN> extends RichMapFunction<IN, byte[]> {
    final SerializationSchema<IN> serializer;

    public SerializeMapFunction(SerializationSchema<IN> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        serializer.open(RuntimeContextInitializationContextAdapters.serializationAdapter(getRuntimeContext()));
    }

    @Override
    public byte[] map(IN value) throws Exception {
        return serializer.serialize(value);
    }
}
