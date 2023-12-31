package com.java.flink.stream.func;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.configuration.Configuration;

public class DeserializeMapFunction<OUT> extends RichMapFunction<byte[], OUT> {
    final DeserializationSchema<OUT> deserializer;

    public DeserializeMapFunction(DeserializationSchema<OUT> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()));
    }

    @Override
    public OUT map(byte[] value) throws Exception {
        return deserializer.deserialize(value);
    }

}
