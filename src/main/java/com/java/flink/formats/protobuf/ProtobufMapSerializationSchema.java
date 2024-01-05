package com.java.flink.formats.protobuf;

import com.alibaba.fastjson2.JSON;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProtobufMapSerializationSchema implements SerializationSchema<Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufMapSerializationSchema.class);
    private final String messageName;
    private final byte[] binaryFileDescriptorSet;
    transient private ProtobufSerializer serializer;

    public ProtobufMapSerializationSchema(String messageName, byte[] binaryFileDescriptorSet) {
        this.messageName = messageName;
        this.binaryFileDescriptorSet = binaryFileDescriptorSet;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(binaryFileDescriptorSet, messageName);
        serializer = new ProtobufSerializer(descriptor);
    }

    @Override
    public byte[] serialize(Map<String, Object> map) {
        try {
            return serializer.serialize(map);
        } catch (Exception e) {
            LOG.error(String.format("proto serialize失败for:%s", JSON.toJSONString(map)), e);
            return null;
        }
    }
}
