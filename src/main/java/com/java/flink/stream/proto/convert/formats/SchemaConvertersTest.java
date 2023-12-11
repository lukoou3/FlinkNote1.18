package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.java.flink.stream.proto.SimpleMessageProtos;
import com.java.flink.stream.proto.convert.types.StructType;
import com.java.flink.stream.proto.convert.formats.SchemaConverters.MessageConverter;
import org.junit.Test;

public class SchemaConvertersTest {

    @Test
    public void testSimpleMessageJavaTypesRead() throws Exception{
        SimpleMessageProtos.SimpleMessageJavaTypes data = SimpleMessageProtos.SimpleMessageJavaTypes.newBuilder()
                .setId(1)
                .setStringValue("莫南")
                .setInt32Value(2)
                .setInt64Value(3)
                .setDoubleValue(10.8)
                .setFloatValue(10.6f)
                .setBoolValue(true)
                .setBytesValue(ByteString.copyFromUtf8("燕青丝"))
                .build();

        byte[] bytes = data.toByteArray();

        String path = getClass().getResource("/protobuf/simple_message.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "SimpleMessageJavaTypes"
        );
        StructType structType = SchemaConverters.toStructType(descriptor);
        System.out.println(structType.treeString());
        MessageConverter messageConverter = new MessageConverter(descriptor, structType);

    }

}
