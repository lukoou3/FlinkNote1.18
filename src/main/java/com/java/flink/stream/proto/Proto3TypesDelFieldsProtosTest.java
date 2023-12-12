package com.java.flink.stream.proto;

import com.google.common.base.Preconditions;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.java.flink.stream.proto.Proto3TypesProtos.Proto3Types;
import com.java.flink.stream.proto.Proto3TypesProtos.StructMessage;
import com.java.flink.stream.proto.Proto3TypesDelFieldsProtos.Proto3TypesDelFields;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class Proto3TypesDelFieldsProtosTest {

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

        Proto3TypesDelFields deserializeData = Proto3TypesDelFields.parseFrom(bytes);

        String dataStr = JsonFormat.printer().print(data);
        String deserializeDataStr = JsonFormat.printer().print(deserializeData);
        System.out.println(dataStr);
        System.out.println(deserializeDataStr);
        System.out.println(String.format("unknownFields:(%s)", deserializeData.getUnknownFields()) );
    }

    @Test
    public void testProto3TypesNotReadUnknownFields() throws Exception{
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

        Proto3TypesDelFields deserializeData = DiscardUnknownFieldsParser.wrap(Proto3TypesDelFields.parser()).parseFrom(bytes);

        String dataStr = JsonFormat.printer().print(data);
        String deserializeDataStr = JsonFormat.printer().print(deserializeData);
        System.out.println(dataStr);
        System.out.println(deserializeDataStr);
        System.out.println(String.format("unknownFields:(%s)", deserializeData.getUnknownFields()) );
    }

    @Test
    public void testProto3TypesReadByHand() throws Exception{
        Descriptors.Descriptor descriptor = Proto3TypesDelFields.getDescriptor();
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

            boolean unknown = false;
            boolean packed = false;
            if (field == null) {
                unknown = true; // Unknown field.
            } else if(wireType == field.getLiteType().getWireType()){
                packed = false;
            }else if (field.isPackable() && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                packed = true;
            } else {
                unknown = true; // Unknown wire type.
            }

            if (unknown) { // Unknown field or wrong wire type.  Skip.
                input.skipField(tag);
                continue;
            }

            Descriptors.FieldDescriptor.Type type = field.getType();
            SimpleMessageProtosTest.Utf8Validation utf8Validation = field.needsUtf8Check()? SimpleMessageProtosTest.Utf8Validation.STRICT: SimpleMessageProtosTest.Utf8Validation.LAZY;
            final Object value;

            if (packed) {
                final int length = input.readRawVarint32();
                final int limit = input.pushLimit(length);
                List<Object> array = new ArrayList<>();
                if (field.getLiteType() == WireFormat.FieldType.ENUM) {
                    while (input.getBytesUntilLimit() > 0) {
                        array.add(input.readEnum());
                    }
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
