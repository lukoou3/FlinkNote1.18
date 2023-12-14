package com.java.flink.stream.proto;

import com.google.common.base.Preconditions;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.java.flink.stream.proto.Proto3TypesProtos.Proto3Types;
import com.java.flink.stream.proto.Proto3TypesProtos.StructMessage;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * protoc --descriptor_set_out=simple_message.desc --java_out=./ simple_message.proto
 *
 * protoc --descriptor_set_out=proto3_types.desc --java_out=./ proto3_types.proto
 *
 * protoc --descriptor_set_out=proto3_types_del_fields.desc --java_out=./ proto3_types_del_fields.proto
 */
public class Proto3TypesProtosTest {

    @Test
    public void testProto3Types() throws Exception{
        StructMessage message = StructMessage.newBuilder().setId(1).setName("燕青丝").setAge(20).setScore(90).build();
        Proto3Types data = Proto3Types.newBuilder()
                .setInt(1)
                .setText("莫南")
                .setEnumValValue(1)
                .setMessage(message)
                .setOptionalInt(2)
                .setOptionalText("苏流沙")
                .setOptionalEnumValValue(1)
                .setOptionalMessage(message)
                .addRepeatedNum(1).addRepeatedNum( 2)
                .addRepeatedMessage(message).addRepeatedMessage(message)
                .putMap("key1", "value1").putMap("key2", "value2")
                .build();
        byte[] bytes = data.toByteArray();

        Proto3Types deserializeData = Proto3Types.parseFrom(bytes);

        String dataStr = JsonFormat.printer().print(data);
        String deserializeDataStr = JsonFormat.printer().print(deserializeData);
        System.out.println(dataStr);
        System.out.println(deserializeDataStr);
    }

    @Test
    public void testProto3TypesDynamicMessage() throws Exception{
        Descriptors.Descriptor descriptor = Proto3Types.getDescriptor();
        System.out.println(descriptor.getFile().getSyntax());
        System.out.println(descriptor.getFile().toProto().getSyntax());
        Descriptors.EnumDescriptor enumDescriptor = descriptor.getEnumTypes().get(0);
        DynamicMessage structMessage = DynamicMessage.newBuilder(StructMessage.getDescriptor())
                .setField(StructMessage.getDescriptor().getFields().get(0), 1L)
                .setField(StructMessage.getDescriptor().getFields().get(1), "燕青丝")
                .setField(StructMessage.getDescriptor().getFields().get(2), 20)
                .setField(StructMessage.getDescriptor().getFields().get(3), 90.0)
                .build();
        Descriptors.Descriptor mapDesc = descriptor.getNestedTypes().get(0);
        DynamicMessage map1 = DynamicMessage.newBuilder(mapDesc)
                .setField(mapDesc.getFields().get(0), "key1")
                .setField(mapDesc.getFields().get(1), "value1")
                .build();
        DynamicMessage map2 = DynamicMessage.newBuilder(mapDesc)
                .setField(mapDesc.getFields().get(0), "key2")
                .setField(mapDesc.getFields().get(1), "value2")
                .build();

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        System.out.println(fields.get(0).getName());
        // 类型匹配
        // com.google.protobuf.FieldSet.isValidType
        builder.setField(fields.get(0), 0L);
        builder.setField(fields.get(1), "莫南");
        builder.setField(fields.get(2), enumDescriptor.findValueByNumber(1));
        builder.setField(fields.get(3), structMessage);
        builder.setField(fields.get(4), 0L);
        builder.setField(fields.get(5), "苏流沙");
        builder.setField(fields.get(6), enumDescriptor.findValueByNumber(1));
        builder.setField(fields.get(7), structMessage);

        builder.setField(fields.get(8), Arrays.asList(1L, 2L));
        builder.setField(fields.get(9), Arrays.asList(structMessage, structMessage));
        builder.setField(fields.get(10), Arrays.asList(map1, map2) );

        DynamicMessage message = builder.build();

        byte[] bytes = message.toByteArray();
        DynamicMessage deserializeMessage = DynamicMessage.parseFrom(descriptor, bytes);

        String messageStr = JsonFormat.printer().print(message);
        String deserializeMessageStr = JsonFormat.printer().print(deserializeMessage);
        System.out.println(messageStr);
        System.out.println(deserializeMessageStr);
    }

    @Test
    public void testProto3TypesReadDynamicMessage() throws Exception {
        Descriptors.Descriptor descriptor = Proto3Types.getDescriptor();
        StructMessage message = StructMessage.newBuilder().setId(1).setName("燕青丝").setAge(20).setScore(90).build();
        Proto3Types data = Proto3Types.newBuilder()
                .setInt(1)
                .setText("莫南")
                .setEnumValValue(1)
                .setMessage(message)
                .setOptionalInt(2)
                .setOptionalText("苏流沙")
                .setOptionalEnumValValue(1)
                .setOptionalMessage(message)
                .addRepeatedNum(1).addRepeatedNum( 2)
                .addRepeatedMessage(message).addRepeatedMessage(message)
                .putMap("key1", "value1").putMap("key2", "value2")
                .build();
        byte[] bytes = data.toByteArray();

        DynamicMessage deserializeMessage = DynamicMessage.parseFrom(descriptor, bytes);

        String dataStr = JsonFormat.printer().print(data);
        String deserializeMessageStr = JsonFormat.printer().print(deserializeMessage);
        System.out.println(dataStr);
        System.out.println(deserializeMessageStr);
    }

    @Test
    public void testProto3TypesReadByHand() throws Exception{
        Descriptors.Descriptor descriptor = Proto3Types.getDescriptor();
        StructMessage message = StructMessage.newBuilder().setId(1).setName("燕青丝").setAge(20).setScore(90).build();
        StructMessage message2 = StructMessage.newBuilder().setId(2).setName("沐璇音").setAge(20).setScore(92).build();
        Proto3Types data = Proto3Types.newBuilder()
                .setInt(1)
                .setText("莫南")
                .setEnumValValue(1)
                .setMessage(message)
                .setOptionalInt(2)
                .setOptionalText("苏流沙")
                .setOptionalEnumValValue(1)
                .setOptionalMessage(message)
                .addRepeatedNum(1).addRepeatedNum( 2)
                .addRepeatedMessage(message).addRepeatedMessage(message2)
                .putMap("key1", "value1").putMap("key2", "value2")
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
            //Preconditions.checkArgument(!field.isPackable());
            //Preconditions.checkArgument(wireType  == field.getLiteType().getWireType());

            boolean packed = false;
            if(wireType == field.getLiteType().getWireType()){
                packed = false;
            }else if (field.isPackable() && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                packed = true;
            }

            Descriptors.FieldDescriptor.Type type = field.getType();
            SimpleMessageProtosTest.Utf8Validation utf8Validation = field.needsUtf8Check()? SimpleMessageProtosTest.Utf8Validation.STRICT: SimpleMessageProtosTest.Utf8Validation.LAZY;
            final Object value;

            if (packed) {
                final int length = input.readRawVarint32();
                final int limit = input.pushLimit(length);
                List<Object> array = new ArrayList<>();
                if (field.getLiteType() == WireFormat.FieldType.ENUM) {
                    array.add(input.readEnum());
                }else {
                    while (input.getBytesUntilLimit() > 0) {
                        array.add(readPrimitiveField( input, field.getLiteType(), utf8Validation));
                    }
                }
                value = array;
                input.popLimit(limit);
            }else{
                switch (field.getType()) {
                    case ENUM:
                        value = input.readEnum();
                        break;
                    case MESSAGE:
                        value = readMessage(input, field);
                        break;
                    default:
                        value = readPrimitiveField(input, field.getLiteType(), utf8Validation);
                }
            }

            String name = field.getName();
            System.out.println(name + " -> " + value);
            if(!field.isRepeated()){
                result.put(name, value);
            }else{
                List<Object> array = (List<Object>)result.get(name);
                if(array == null){
                    array = new ArrayList<>();
                    result.put(name, array);
                }
                array.add(value);
                System.out.println(name + " -> " + array);
            }
        }

        String dataStr = JsonFormat.printer().print(data);
        System.out.println(dataStr);
        System.out.println(result);
    }

    static Object readMessage(CodedInputStream input, Descriptors.FieldDescriptor field) throws Exception{
        Descriptors.Descriptor descriptor = field.getMessageType();

        final int length = input.readRawVarint32();
        final int oldLimit = input.pushLimit(length);

        Object message = readMessage(input, descriptor);

        input.checkLastTagWas(0);
        if (input.getBytesUntilLimit() != 0) {
            throw new RuntimeException("parse");
        }
        input.popLimit(oldLimit);

        return message;
    }

    static Object readMessage(CodedInputStream input, Descriptors.Descriptor descriptor) throws Exception{
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
            SimpleMessageProtosTest.Utf8Validation utf8Validation = field.needsUtf8Check()? SimpleMessageProtosTest.Utf8Validation.STRICT: SimpleMessageProtosTest.Utf8Validation.LAZY;
            final Object value;
            switch (field.getType()) {
                case ENUM:
                    value = input.readEnum();
                    break;
                case MESSAGE:
                    value = readMessage(input, field);
                    break;
                default:
                    value = readPrimitiveField(input, field.getLiteType(), utf8Validation);
            }


            String name = field.getName();
            result.put(name, value);
        }

        return result;
    }

    static Object readPrimitiveField(
            CodedInputStream input, WireFormat.FieldType type, SimpleMessageProtosTest.Utf8Validation utf8Validation) throws IOException {
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
