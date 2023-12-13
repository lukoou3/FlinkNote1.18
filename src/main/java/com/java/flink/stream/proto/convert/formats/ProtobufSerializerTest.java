package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.java.flink.stream.proto.convert.types.StructType;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtobufSerializerTest {

    @Test
    public void testSimpleMessageJavaTypes() throws Exception{
        String path = getClass().getResource("/protobuf/simple_message.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "SimpleMessageJavaTypes"
        );

        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        Map<String, Object> data = new HashMap<>();
        data.put(fields.get(0).getName(), 1L);
        data.put(fields.get(1).getName(), "莫南");
        data.put(fields.get(2).getName(), 2);
        data.put(fields.get(3).getName(), 3L);
        data.put(fields.get(4).getName(), 10.8);
        data.put(fields.get(5).getName(), 10.6f);
        data.put(fields.get(6).getName(), true);
        //data.put(fields.get(7).getName(), ByteString.copyFromUtf8("燕青丝"));
        data.put(fields.get(7).getName(), ByteString.copyFromUtf8("燕青丝").toByteArray());

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytes = serializer.serialize(data);

        StructType structType = SchemaConverters.toStructType(descriptor);
        SchemaConverters.MessageConverter messageConverter = new SchemaConverters.MessageConverter(descriptor, structType);
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);

        System.out.println(data);
        System.out.println(result);
    }

}
