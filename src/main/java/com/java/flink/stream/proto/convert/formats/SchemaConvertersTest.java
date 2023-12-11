package com.java.flink.stream.proto.convert.formats;

import com.alibaba.fastjson2.JSON;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.util.JsonFormat;
import com.java.flink.stream.proto.Proto3Types2Protos;
import com.java.flink.stream.proto.SimpleMessageProtos;
import com.java.flink.stream.proto.convert.types.ArrayType;
import com.java.flink.stream.proto.convert.types.StructType;
import com.java.flink.stream.proto.convert.formats.SchemaConverters.MessageConverter;
import com.java.flink.stream.proto.convert.types.Types;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

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

        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);
        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);
    }

    @Test
    public void testSimpleMessageJavaTypesReadPartialField() throws Exception{
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
        StructType.StructField[] fields = structType.fields;
        int[] indx = {2, 4, 6, 7};
        structType = new StructType(Arrays.stream(indx).mapToObj(i -> fields[i-1]).toArray(StructType.StructField[]::new));
        System.out.println(structType.treeString());
        MessageConverter messageConverter = new MessageConverter(descriptor, structType);

        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);
        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);
    }

    @Test
    public void testSimpleMessageRead() throws Exception{
        SimpleMessageProtos.SimpleMessage data = SimpleMessageProtos.SimpleMessage.newBuilder()
                .setId(1)
                .setStringValue("莫南")
                .setInt32Value(3)
                .setUint32Value(4)
                .setSint32Value(5)
                .setFixed32Value(6)
                .setSfixed32Value(7)
                .setInt64Value(8)
                .setUint64Value(9)
                .setSint64Value(10)
                .setFixed64Value(11)
                .setSfixed64Value(12)
                .setDoubleValue(13)
                .setFloatValue(14)
                .setBoolValue(true)
                .setBytesValue(ByteString.copyFromUtf8("燕青丝"))
                .build();

        byte[] bytes = data.toByteArray();

        String path = getClass().getResource("/protobuf/simple_message.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "SimpleMessage"
        );
        StructType structType = SchemaConverters.toStructType(descriptor);
        System.out.println(structType.treeString());
        MessageConverter messageConverter = new MessageConverter(descriptor, structType);

        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);
        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);
    }

    @Test
    public void testSimpleMessageReadPartialField() throws Exception{
        SimpleMessageProtos.SimpleMessage data = SimpleMessageProtos.SimpleMessage.newBuilder()
                .setId(1)
                .setStringValue("莫南")
                .setInt32Value(3)
                .setUint32Value(4)
                .setSint32Value(5)
                .setFixed32Value(6)
                .setSfixed32Value(7)
                .setInt64Value(8)
                .setUint64Value(9)
                .setSint64Value(10)
                .setFixed64Value(11)
                .setSfixed64Value(12)
                .setDoubleValue(13)
                .setFloatValue(14)
                .setBoolValue(true)
                .setBytesValue(ByteString.copyFromUtf8("燕青丝"))
                .build();

        byte[] bytes = data.toByteArray();

        String path = getClass().getResource("/protobuf/simple_message.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "SimpleMessage"
        );
        StructType structType = SchemaConverters.toStructType(descriptor);
        StructType.StructField[] fields = structType.fields;
        int[] indx = {2, 4, 6, 7, 9, 10, 13};
        structType = new StructType(Arrays.stream(indx).mapToObj(i -> fields[i-1]).toArray(StructType.StructField[]::new));
        System.out.println(structType.treeString());
        MessageConverter messageConverter = new MessageConverter(descriptor, structType);

        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);
        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);
    }

    @Test
    public void testProto3Types2Read() throws Exception{
        Proto3Types2Protos.StructMessage message = Proto3Types2Protos.StructMessage.newBuilder().setId(1).setName("燕青丝").setAge(20).setScore(90).build();
        Proto3Types2Protos.StructMessage message2 = Proto3Types2Protos.StructMessage.newBuilder().setId(2).setName("沐璇音").setAge(20).setScore(92).build();
        Proto3Types2Protos.Proto3Types2 data = Proto3Types2Protos.Proto3Types2.newBuilder()
                .setInt(1)
                .setText("莫南")
                .setEnumValValue(1)
                .setMessage(message)
                .setOptionalInt(2)
                .setOptionalText("苏流沙")
                .setOptionalEnumValValue(1)
                .setOptionalMessage(message2)
                .addRepeatedNum(1).addRepeatedNum( 2)
                .addRepeatedMessage(message).addRepeatedMessage(message2)
                .build();

        byte[] bytes = data.toByteArray();

        String path = getClass().getResource("/protobuf/proto3_types2.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "Proto3Types2"
        );
        StructType structType = SchemaConverters.toStructType(descriptor);
        System.out.println(structType.treeString());
        MessageConverter messageConverter = new MessageConverter(descriptor, structType);

        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);
        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(JSON.toJSONString(result));
    }

    @Test
    public void testProto3Types2ReadPartialField() throws Exception{
        Proto3Types2Protos.StructMessage message = Proto3Types2Protos.StructMessage.newBuilder().setId(1).setName("燕青丝").setAge(20).setScore(90).build();
        Proto3Types2Protos.StructMessage message2 = Proto3Types2Protos.StructMessage.newBuilder().setId(2).setName("沐璇音").setAge(20).setScore(92).build();
        Proto3Types2Protos.Proto3Types2 data = Proto3Types2Protos.Proto3Types2.newBuilder()
                .setInt(1)
                .setText("莫南")
                .setEnumValValue(1)
                .setMessage(message)
                .setOptionalInt(2)
                .setOptionalText("苏流沙")
                .setOptionalEnumValValue(1)
                .setOptionalMessage(message2)
                .addRepeatedNum(1).addRepeatedNum( 2)
                .addRepeatedMessage(message).addRepeatedMessage(message2)
                .build();

        byte[] bytes = data.toByteArray();

        String path = getClass().getResource("/protobuf/proto3_types2.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "Proto3Types2"
        );
        StructType structType = SchemaConverters.toStructType(descriptor);
        StructType.StructField[] fields = structType.fields;
        int[] indx = {2, 4, 6, 7, 9, 10};
        structType = new StructType(Arrays.stream(indx).mapToObj(i -> fields[i-1]).toArray(StructType.StructField[]::new));
        StructType msgStructType = new StructType(new StructType.StructField[]{
                new StructType.StructField("name", Types.STRING),
                new StructType.StructField("age", Types.INT),
        });
        structType.fields[1] = new StructType.StructField(structType.fields[1].name, msgStructType);
        structType.fields[structType.fields.length - 1] = new StructType.StructField(structType.fields[structType.fields.length - 1].name, new ArrayType(msgStructType));
        System.out.println(structType.treeString());
        MessageConverter messageConverter = new MessageConverter(descriptor, structType);

        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);
        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(JSON.toJSONString(result));
    }
}
