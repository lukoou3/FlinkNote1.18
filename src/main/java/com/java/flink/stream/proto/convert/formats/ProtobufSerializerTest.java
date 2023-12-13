package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.java.flink.stream.proto.convert.types.StructType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
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

    @Test
    public void testSimpleMessageJavaTypesPartialField() throws Exception{
        String path = getClass().getResource("/protobuf/simple_message.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "SimpleMessageJavaTypes"
        );

        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        Map<String, Object> data = new HashMap<>();
        data.put(fields.get(0).getName(), 1L);
        data.put(fields.get(1).getName(), "莫南");
        //data.put(fields.get(2).getName(), 2);
        data.put(fields.get(3).getName(), 3L);
        //data.put(fields.get(4).getName(), 10.8);
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

        System.out.println(StringUtils.repeat("#", 100));

        data = new HashMap<>();
        data.put(fields.get(0).getName(), 1L);
        data.put(fields.get(1).getName(), "莫南");
        data.put(fields.get(5).getName(), 10.6f);

        bytes = serializer.serialize(data);
        input = CodedInputStream.newInstance(bytes);
        result = messageConverter.converter(input);
        System.out.println(data);
        System.out.println(result);
    }

    @Test
    public void testProto3Types2() throws Exception{
        String path = getClass().getResource("/protobuf/proto3_types2.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "Proto3Types2"
        );

        Map<String, Object> message = new HashMap<>();
        message.put("id", 1);
        message.put("name", "燕青丝");
        message.put("age", 18);
        message.put("score", 92);
        Map<String, Object> message2 = new HashMap<>();
        message2.put("id", 2L);
        message2.put("name", "苏流沙");
        //message2.put("age", 20);
        message2.put("score", 86);

        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        Map<String, Object> data = new HashMap<>();
        data.put("int", 1L);
        data.put("text", "莫南");
        data.put("enum_val", 1);
        data.put("message", message);
        data.put("optional_int", 2);
        data.put("optional_text", "苏流沙");
        data.put("optional_enum_val", 1);
        data.put("optional_message", message2);
        data.put("repeated_num", Arrays.asList(1, 2));
        data.put("repeated_message", Arrays.asList(message, message2));

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
