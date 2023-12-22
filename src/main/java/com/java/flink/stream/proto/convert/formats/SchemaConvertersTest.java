package com.java.flink.stream.proto.convert.formats;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.util.JsonFormat;
import com.java.flink.stream.proto.Proto3Types2Protos;
import com.java.flink.stream.proto.SimpleMessageProtos;
import com.java.flink.stream.proto.convert.types.ArrayType;
import com.java.flink.stream.proto.convert.types.DataType;
import com.java.flink.stream.proto.convert.types.StructType;
import com.java.flink.stream.proto.convert.formats.SchemaConverters.MessageConverter;
import com.java.flink.stream.proto.convert.types.Types;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SchemaConvertersTest {

    @Test
    public void testSimpleMessageDataTypeCheckMatch() throws Exception{
        String path = getClass().getResource("/protobuf/simple_message.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "SimpleMessage"
        );
        StructType geneStructType = SchemaConverters.toStructType(descriptor);
        System.out.println(geneStructType.treeString());
        System.out.println(geneStructType.simpleString());
        SchemaConverters.checkMatch(descriptor, geneStructType);

        StructType dataType1 = (StructType)Types.parseDataType("struct<id:bigint, string_value:string, int32_value:int, uint32_value:int, sint32_value:int, fixed32_value:int, sfixed32_value:int, int64_value:bigint, uint64_value:bigint, sint64_value:bigint, fixed64_value:bigint, sfixed64_value:bigint, double_value:double, float_value:float, bool_value:boolean, bytes_value:binary>");
        SchemaConverters.checkMatch(descriptor, dataType1);

        // 减少字段
        StructType dataType2 = (StructType)Types.parseDataType("struct<id:bigint, string_value:string, sint32_value:int, fixed32_value:int, sfixed32_value:int, int64_value:bigint, uint64_value:bigint, sint64_value:bigint, fixed64_value:bigint, sfixed64_value:bigint, float_value:float, bool_value:boolean>");
        SchemaConverters.checkMatch(descriptor, dataType2);

        // 字段名不存在
        StructType dataType3 = (StructType)Types.parseDataType("struct<id:bigint, string_value2:string, sint32_value:int, fixed32_value:int, sfixed32_value:int, int64_value:bigint, uint64_value:bigint, sint64_value:bigint, fixed64_value:bigint, sfixed64_value:bigint, float_value:float, bool_value:boolean>");
        try {
            SchemaConverters.checkMatch(descriptor, dataType3);
        } catch (Exception e) {
            System.out.println("字段名不存在:");
            System.out.println(e);
        }

        // 字段类型转换
        StructType dataType4 = (StructType)Types.parseDataType("struct<id:int, string_value:string, sint32_value:bigint, fixed32_value:int, sfixed32_value:int, int64_value:bigint, uint64_value:bigint, sint64_value:bigint, fixed64_value:bigint, sfixed64_value:bigint, float_value:float, bool_value:boolean>");
        SchemaConverters.checkMatch(descriptor, dataType4);

        // 字段类型不匹配
        StructType dataType5 = (StructType)Types.parseDataType("struct<id:bigint, string_value:int, sint32_value:int, fixed32_value:int, sfixed32_value:int, int64_value:bigint, uint64_value:bigint, sint64_value:bigint, fixed64_value:bigint, sfixed64_value:bigint, float_value:float, bool_value:boolean>");
        try {
            SchemaConverters.checkMatch(descriptor, dataType5);
        } catch (Exception e) {
            System.out.println("字段类型不匹配:");
            System.out.println(e);
        }
    }

    @Test
    public void testProto3Types2DataTypeCheckMatch() throws Exception{
        String path = getClass().getResource("/protobuf/proto3_types2.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "Proto3Types2"
        );
        StructType geneStructType = SchemaConverters.toStructType(descriptor);
        System.out.println(geneStructType.treeString());
        System.out.println(geneStructType.simpleString());
        SchemaConverters.checkMatch(descriptor, geneStructType);

        StructType dataType1 = (StructType)Types.parseDataType("struct<int:bigint, text:string, enum_val:int, message:struct<id:bigint, name:string, age:int, score:double>, optional_int:bigint, optional_text:string, optional_enum_val:int, optional_message:struct<id:bigint, name:string, age:int, score:double>, repeated_num:array<bigint>, repeated_message:array<struct<id:bigint, name:string, age:int, score:double>>>");
        SchemaConverters.checkMatch(descriptor, dataType1);

        // 减少字段
        StructType dataType2 = (StructType)Types.parseDataType("struct<int:bigint,  enum_val:int, message:struct<id:bigint, name:string, score:double>, optional_int:bigint, optional_text:string, optional_enum_val:int, optional_message:struct<id:bigint,  age:int, score:double>, repeated_num:array<bigint>, repeated_message:array<struct<id:bigint, name:string, age:int, score:double>>>");
        SchemaConverters.checkMatch(descriptor, dataType2);

        // 字段名不存在
        StructType dataType3 = (StructType)Types.parseDataType("struct<int:bigint, text:string, enum_val:int, message:struct<id:bigint, name:string, age:int, score:double>, optional_int:bigint, optional_text:string, optional_enum_val:int, optional_message:struct<id:bigint, name:string, age2:int, score:double>, repeated_num:array<bigint>, repeated_message:array<struct<id:bigint, name:string, age:int, score:double>>>");
        try {
            SchemaConverters.checkMatch(descriptor, dataType3);
        } catch (Exception e) {
            System.out.println("字段名不存在:");
            System.out.println(e);
        }

        // 字段类型转换
        StructType dataType4 = (StructType)Types.parseDataType("struct<int:int, text:string, enum_val:int, message:struct<id:int, name:string, age:double, score:int>, optional_int:bigint, optional_text:string, optional_enum_val:int, optional_message:struct<id:int, name:string, age:int, score:double>, repeated_num:array<bigint>, repeated_message:array<struct<id:int, name:string, age:double, score:int>>>");
        SchemaConverters.checkMatch(descriptor, dataType4);

        // 字段类型不匹配
        StructType dataType5 = (StructType)Types.parseDataType("struct<int:bigint, text:string, enum_val:int, message:struct<id:bigint, name:string, age:int, score:double>, optional_int:bigint, optional_text:string, optional_enum_val:int, optional_message:struct<id:bigint, name:string, age:int, score:double>, repeated_num:array<bigint>, repeated_message:array<struct<id:bigint, name:int, age:int, score:double>>>");
        try {
            SchemaConverters.checkMatch(descriptor, dataType5);
        } catch (Exception e) {
            System.out.println("字段类型不匹配:");
            System.out.println(e);
        }
    }

    @Test
    public void testFieldHasDefaultValue() throws Exception {
        String path = getClass().getResource("/protobuf/proto3_types2.desc").getPath();
        Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "Proto3Types2"
        );
        List<Descriptors.FieldDescriptor> fds = descriptor.getFields();
        for (int i = 0; i < fds.size(); i++) {
            Descriptors.FieldDescriptor fd = fds.get(i);
            if(fd.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                // fd.hasDefaultValue() 是显式定义默认值
                System.out.println(fd.getName() + ":" + fd.hasDefaultValue() + "," + fd.getDefaultValue() + "," + fd.getDefaultValue().getClass());
            }
        }
    }

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

    @Test
    public void testProto3Types2EmitDefaultValues() throws Exception{
        Proto3Types2Protos.StructMessage message = Proto3Types2Protos.StructMessage.newBuilder().setAge(20).setScore(90).build();
        Proto3Types2Protos.StructMessage message2 = Proto3Types2Protos.StructMessage.newBuilder().setId(2).setName("沐璇音").setAge(20).setScore(92).build();
        Proto3Types2Protos.Proto3Types2 data = Proto3Types2Protos.Proto3Types2.newBuilder()
                //.setInt(1)
                //.setText("莫南")
                .setEnumValValue(1)
                .setMessage(message)
                //.setOptionalInt(2)
                //.setOptionalText("苏流沙")
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
        System.out.println(JSON.toJSONString(result, JSONWriter.Feature.PrettyFormat));
        System.out.println(StringUtils.repeat('#', 50));

        messageConverter = new MessageConverter(descriptor, structType, true);
        input = CodedInputStream.newInstance(bytes);
        result = messageConverter.converter(input);
        System.out.println(JSON.toJSONString(result, JSONWriter.Feature.PrettyFormat));
    }
}
