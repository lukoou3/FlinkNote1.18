package com.java.flink.formats.protobuf;

import com.java.flink.types.StructType;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

public class ProtobufMapDeserializationSchema implements DeserializationSchema<Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufMapDeserializationSchema.class);
    private final String messageName;
    private final byte[] binaryFileDescriptorSet;
    private final StructType dataType;
    private final boolean ignoreParseErrors;
    private final boolean emitDefaultValues;
    transient private SchemaConverters.MessageConverter converter;

    public ProtobufMapDeserializationSchema(String messageName, byte[] binaryFileDescriptorSet, StructType dataType, boolean ignoreParseErrors, boolean emitDefaultValues) {
        this.messageName = messageName;
        this.binaryFileDescriptorSet = binaryFileDescriptorSet;
        this.dataType = dataType;
        this.ignoreParseErrors = ignoreParseErrors;
        this.emitDefaultValues = emitDefaultValues;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        Descriptor descriptor = ProtobufUtils.buildDescriptor(binaryFileDescriptorSet, messageName);
        this.converter = new SchemaConverters.MessageConverter(descriptor, dataType, emitDefaultValues);
    }

    @Override
    public Map<String, Object> deserialize(byte[] message) throws IOException {
        if(message == null){
            return null;
        }

        try {
            Map<String, Object> map = converter.converter(message);
            return map;
        } catch (Exception e) {
            LOG.error(String.format("proto解析失败for:%s", Base64.getEncoder().encodeToString(message)), e);
            if(ignoreParseErrors){
                return null;
            }else{
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean isEndOfStream(Map<String, Object> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Map<String, Object>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, Object>>() {});
    }
}
