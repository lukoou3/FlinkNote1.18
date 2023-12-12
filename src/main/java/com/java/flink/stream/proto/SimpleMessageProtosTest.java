package com.java.flink.stream.proto;

import com.google.common.base.Preconditions;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.java.flink.stream.proto.SimpleMessageProtos.SimpleMessageJavaTypes;
import com.java.flink.stream.proto.SimpleMessageProtos.SimpleMessage;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class SimpleMessageProtosTest {

    @Test
    public void testSimpleMessageJavaTypes() throws Exception{
        SimpleMessageJavaTypes data = SimpleMessageJavaTypes.newBuilder()
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

        SimpleMessageJavaTypes deserializeData = SimpleMessageJavaTypes.parseFrom(bytes);

        String dataStr = JsonFormat.printer().print(data);
        String deserializeDataStr = JsonFormat.printer().print(deserializeData);
        System.out.println(dataStr);
        System.out.println(deserializeDataStr);
    }

    @Test
    public void testSimpleMessageJavaTypesWriteByHand() throws Exception{
        Descriptors.Descriptor descriptor = SimpleMessageJavaTypes.getDescriptor();
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        Map<String, Object> data = new HashMap<>();
        data.put(fields.get(0).getName(), 1L);
        data.put(fields.get(1).getName(), "莫南");
        data.put(fields.get(2).getName(), 2);
        data.put(fields.get(3).getName(), 3L);
        data.put(fields.get(4).getName(), 10.8);
        data.put(fields.get(5).getName(), 10.6f);
        data.put(fields.get(6).getName(), true);
        data.put(fields.get(7).getName(), ByteString.copyFromUtf8("燕青丝"));

        byte[] bytes = null;

        SimpleMessageJavaTypes deserializeData = SimpleMessageJavaTypes.parseFrom(bytes);

    }


    @Test
    public void testSimpleMessageJavaTypesDynamicMessage() throws Exception{
        Descriptors.Descriptor descriptor = SimpleMessageJavaTypes.getDescriptor();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        System.out.println(fields.get(0).getName());
        // 类型匹配
        // com.google.protobuf.FieldSet.isValidType
        builder.setField(fields.get(0), 1L);
        builder.setField(fields.get(1), "莫南");
        builder.setField(fields.get(2), 2);
        builder.setField(fields.get(3), 3L);
        builder.setField(fields.get(4), 10.8);
        builder.setField(fields.get(5), 10.6f);
        builder.setField(fields.get(6), true);
        //builder.setField(fields.get(7), "燕青丝".getBytes(StandardCharsets.UTF_8));
        builder.setField(fields.get(7), ByteString.copyFromUtf8("燕青丝"));
        DynamicMessage message = builder.build();

        /**
         * com.google.protobuf.AbstractMessageLite#toByteArray()
         * com.google.protobuf.DynamicMessage#writeTo(com.google.protobuf.CodedOutputStream)
         * com.google.protobuf.FieldSet#writeTo(com.google.protobuf.CodedOutputStream)
         */
        byte[] bytes = message.toByteArray();
        DynamicMessage deserializeMessage = DynamicMessage.parseFrom(descriptor, bytes);

        String messageStr = JsonFormat.printer().print(message);
        String deserializeMessageStr = JsonFormat.printer().print(deserializeMessage);
        System.out.println(messageStr);
        System.out.println(deserializeMessageStr);
    }

    @Test
    public void testSimpleMessageJavaTypesReadByHand() throws Exception{
        Descriptors.Descriptor descriptor = SimpleMessageJavaTypes.getDescriptor();
        SimpleMessageJavaTypes data = SimpleMessageJavaTypes.newBuilder()
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

        // com.google.protobuf.AbstractMessageLite.Builder.mergeFrom(byte[], int, int)
        // com.google.protobuf.MessageReflection.mergeMessageFrom
        CodedInputStream input = CodedInputStream.newInstance(bytes);

        Map<String, Object> result = new LinkedHashMap<>();
        while (true) {
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            final int wireType = WireFormat.getTagWireType(tag);
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);

            final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
            Preconditions.checkArgument(!field.isPackable());
            Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());
            Descriptors.FieldDescriptor.Type type = field.getType();
            Utf8Validation utf8Validation = field.needsUtf8Check()? Utf8Validation.STRICT:Utf8Validation.LAZY;
            final Object value = readPrimitiveField(input, field.getLiteType(), utf8Validation);

            String name = field.getName();
            result.put(name, value);
        }

        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);

        input = CodedInputStream.newInstance(bytes);
        result = new LinkedHashMap<>();
        int i = 0;
        while (true) {
            i ++;
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            if(i == 3 || i == 5 || i == 8){
                input.skipField(tag);
                continue;
            }

            final int wireType = WireFormat.getTagWireType(tag);
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);

            final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
            Preconditions.checkArgument(!field.isPackable());
            Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());
            Descriptors.FieldDescriptor.Type type = field.getType();
            Utf8Validation utf8Validation = field.needsUtf8Check()? Utf8Validation.STRICT:Utf8Validation.LAZY;
            final Object value = readPrimitiveField(input, field.getLiteType(), utf8Validation);

            String name = field.getName();
            result.put(name, value);
        }
        System.out.println(result);
    }

    @Test
    public void testSimpleMessageJavaTypesReadByHandNotWriteDefaultValue() throws Exception{
        Descriptors.Descriptor descriptor = SimpleMessageJavaTypes.getDescriptor();
        SimpleMessageJavaTypes data = SimpleMessageJavaTypes.newBuilder()
                .setId(0)
                .setStringValue("莫南")
                .setInt32Value(2)
                .setInt64Value(3)
                .setDoubleValue(0)
                .setFloatValue(10.6f)
                .setBoolValue(true)
                .build();

        byte[] bytes = data.toByteArray();

        // com.google.protobuf.AbstractMessageLite.Builder.mergeFrom(byte[], int, int)
        // com.google.protobuf.MessageReflection.mergeMessageFrom
        CodedInputStream input = CodedInputStream.newInstance(bytes);

        Map<String, Object> result = new LinkedHashMap<>();
        while (true) {
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            final int wireType = WireFormat.getTagWireType(tag);
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);

            final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
            Preconditions.checkArgument(!field.isPackable());
            Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());
            Descriptors.FieldDescriptor.Type type = field.getType();
            Utf8Validation utf8Validation = field.needsUtf8Check()? Utf8Validation.STRICT:Utf8Validation.LAZY;
            final Object value = readPrimitiveField(input, field.getLiteType(), utf8Validation);

            String name = field.getName();
            result.put(name, value);
        }

        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);
    }

    @Test
    public void testSimpleMessage() throws Exception{
        SimpleMessage data = SimpleMessage.newBuilder()
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

        // 也会读，最后一个字段不匹配，读取逻辑是写死的
        //SimpleMessageJavaTypes deserializeData = SimpleMessageJavaTypes.parseFrom(bytes);
        SimpleMessage deserializeData = SimpleMessage.parseFrom(bytes);

        String dataStr = JsonFormat.printer().print(data);
        String deserializeDataStr = JsonFormat.printer().print(deserializeData);
        System.out.println(dataStr);
        System.out.println(deserializeDataStr);
    }

    @Test
    public void testSimpleMessageReadByHand() throws Exception{
        Descriptors.Descriptor descriptor = SimpleMessage.getDescriptor();
        SimpleMessage data = SimpleMessage.newBuilder()
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

        // com.google.protobuf.AbstractMessageLite.Builder.mergeFrom(byte[], int, int)
        // com.google.protobuf.MessageReflection.mergeMessageFrom
        CodedInputStream input = CodedInputStream.newInstance(bytes);

        Map<String, Object> result = new LinkedHashMap<>();
        while (true) {
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            final int wireType = WireFormat.getTagWireType(tag);
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);

            final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
            Preconditions.checkArgument(!field.isPackable());
            Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());
            Descriptors.FieldDescriptor.Type type = field.getType();
            Utf8Validation utf8Validation = field.needsUtf8Check()? Utf8Validation.STRICT:Utf8Validation.LAZY;
            final Object value = readPrimitiveField(input, field.getLiteType(), utf8Validation);

            String name = field.getName();
            result.put(name, value);
        }

        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);

        input = CodedInputStream.newInstance(bytes);
        result = new LinkedHashMap<>();
        int i = 0;
        while (true) {
            i ++;
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            if(i == 3 || i == 5 || i == 8){
                input.skipField(tag);
                continue;
            }

            final int wireType = WireFormat.getTagWireType(tag);
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);

            final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
            Preconditions.checkArgument(!field.isPackable());
            Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());
            Descriptors.FieldDescriptor.Type type = field.getType();
            Utf8Validation utf8Validation = field.needsUtf8Check()? Utf8Validation.STRICT:Utf8Validation.LAZY;
            final Object value = readPrimitiveField(input, field.getLiteType(), utf8Validation);

            String name = field.getName();
            result.put(name, value);
        }
        System.out.println(result);
    }

    @Test
    public void testSimpleMessageReadByHandReadFile() throws Exception {
        Descriptors.Descriptor descriptorBuildIn = SimpleMessage.getDescriptor();
        byte[] byteArray = IOUtils.resourceToByteArray("/protobuf/simple_message.desc");
        Descriptors.Descriptor descriptor = buildDescriptor(byteArray, "SimpleMessage");

        SimpleMessage data = SimpleMessage.newBuilder()
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

        // com.google.protobuf.AbstractMessageLite.Builder.mergeFrom(byte[], int, int)
        // com.google.protobuf.MessageReflection.mergeMessageFrom
        CodedInputStream input = CodedInputStream.newInstance(bytes);

        Map<String, Object> result = new LinkedHashMap<>();
        while (true) {
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            final int wireType = WireFormat.getTagWireType(tag);
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);

            final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
            Preconditions.checkArgument(!field.isPackable());
            Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());
            Descriptors.FieldDescriptor.Type type = field.getType();
            Utf8Validation utf8Validation = field.needsUtf8Check()? Utf8Validation.STRICT:Utf8Validation.LAZY;
            final Object value = readPrimitiveField(input, field.getLiteType(), utf8Validation);

            String name = field.getName();
            result.put(name, value);
        }

        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);

        input = CodedInputStream.newInstance(bytes);
        result = new LinkedHashMap<>();
        int i = 0;
        while (true) {
            i ++;
            int tag = input.readTag();
            if (tag == 0) {
                break;
            }

            if(i == 3 || i == 5 || i == 8){
                input.skipField(tag);
                continue;
            }

            final int wireType = WireFormat.getTagWireType(tag);
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);

            final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
            Preconditions.checkArgument(!field.isPackable());
            Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());
            Descriptors.FieldDescriptor.Type type = field.getType();
            Utf8Validation utf8Validation = field.needsUtf8Check()? Utf8Validation.STRICT:Utf8Validation.LAZY;
            final Object value = readPrimitiveField(input, field.getLiteType(), utf8Validation);

            String name = field.getName();
            result.put(name, value);
        }
        System.out.println(result);
    }



    public static Descriptors.Descriptor buildDescriptor(byte[] bytes, String messageName)  throws Exception{
        List<Descriptors.FileDescriptor> fileDescriptorList = parseFileDescriptorSet(bytes);
        Descriptors.Descriptor descriptor = fileDescriptorList.stream().flatMap(fileDesc -> {
            return fileDesc.getMessageTypes().stream().filter(desc -> desc.getName().equals(messageName) || desc.getFullName().equals(messageName));
        }).findFirst().get();
        return descriptor;
    }

    public static List<Descriptors.FileDescriptor> parseFileDescriptorSet(byte[] bytes) throws Exception{
        DescriptorProtos.FileDescriptorSet fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(bytes);
        Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap = fileDescriptorSet.getFileList().stream().collect(Collectors.toMap(x -> x.getName(), x -> x));
        List<Descriptors.FileDescriptor> fileDescriptorList = new ArrayList<>();
        for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : fileDescriptorSet.getFileList()) {
            Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor(fileDescriptorProto, fileDescriptorProtoMap);
            fileDescriptorList.add(fileDescriptor);
        }
        return fileDescriptorList;
    }

    private static Descriptors.FileDescriptor buildFileDescriptor(DescriptorProtos.FileDescriptorProto fileDescriptorProto, Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap) throws Exception{
        ProtocolStringList dependencyList = fileDescriptorProto.getDependencyList();
        Descriptors.FileDescriptor[] fileDescriptorArray = new Descriptors.FileDescriptor[dependencyList.size()];
        for (int i = 0; i < fileDescriptorArray.length; i++) {
            String dependency = dependencyList.get(i);
            DescriptorProtos.FileDescriptorProto dependencyProto = fileDescriptorProtoMap.get(dependency);
            if (dependencyProto == null) {
                throw new IllegalArgumentException("dependency:" + dependency + "not exist");
            }
            if (dependencyProto.getName().equals("google/protobuf/any.proto")
                    && dependencyProto.getPackage().equals("google.protobuf")) {
                // For Any, use the descriptor already included as part of the Java dependency.
                // Without this, JsonFormat used for converting Any fields fails when
                // an Any field in input is set to `Any.getDefaultInstance()`.
                fileDescriptorArray[i] = AnyProto.getDescriptor();
            } else {
                fileDescriptorArray[i] = buildFileDescriptor(dependencyProto, fileDescriptorProtoMap);
            }
        }

        return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, fileDescriptorArray);
    }

    static Object readPrimitiveField(
            CodedInputStream input, WireFormat.FieldType type, Utf8Validation utf8Validation) throws IOException {
        switch (type) {
            case DOUBLE:
                return input.readDouble();
            case FLOAT:
                return input.readFloat();
            case INT64:
                return input.readInt64();
            case UINT64:
                return input.readUInt64();
            case INT32:
                return input.readInt32();
            case FIXED64:
                return input.readFixed64();
            case FIXED32:
                return input.readFixed32();
            case BOOL:
                return input.readBool();
            case BYTES:
                return input.readBytes();
            case UINT32:
                return input.readUInt32();
            case SFIXED32:
                return input.readSFixed32();
            case SFIXED64:
                return input.readSFixed64();
            case SINT32:
                return input.readSInt32();
            case SINT64:
                return input.readSInt64();

            case STRING:
                return utf8Validation.readString(input);
            case GROUP:
                throw new IllegalArgumentException("readPrimitiveField() cannot handle nested groups.");
            case MESSAGE:
                throw new IllegalArgumentException("readPrimitiveField() cannot handle embedded messages.");
            case ENUM:
                // We don't handle enums because we don't know what to do if the
                // value is not recognized.
                throw new IllegalArgumentException("readPrimitiveField() cannot handle enums.");
        }

        throw new RuntimeException("There is no way to get here, but the compiler thinks otherwise.");
    }

    enum Utf8Validation {
        /** Eagerly parses to String; silently accepts invalid UTF8 bytes. */
        LOOSE {
            @Override
            Object readString(CodedInputStream input) throws IOException {
                return input.readString();
            }
        },
        /** Eagerly parses to String; throws an IOException on invalid bytes. */
        STRICT {
            @Override
            Object readString(CodedInputStream input) throws IOException {
                return input.readStringRequireUtf8();
            }
        },
        /** Keep data as ByteString; validation/conversion to String is lazy. */
        LAZY {
            @Override
            Object readString(CodedInputStream input) throws IOException {
                return input.readBytes();
            }
        };

        /** Read a string field from the input with the proper UTF8 validation. */
        abstract Object readString(CodedInputStream input) throws IOException;
    }
}
