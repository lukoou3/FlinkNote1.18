package com.java.flink.stream.proto.convert.formats;

import com.java.flink.stream.proto.convert.types.*;
import com.java.flink.stream.proto.convert.types.StructType.StructField;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;
import org.apache.flink.util.Preconditions;

import java.util.*;

public class SchemaConverters {
    public static StructType toStructType(Descriptor descriptor) {
        StructField[] fields = descriptor.getFields().stream().map(f -> structFieldFor(f)).toArray(StructField[]::new);
        return new StructType(fields);
    }

    private static StructField structFieldFor(FieldDescriptor fd) {
        WireFormat.FieldType type = fd.getLiteType();
        DataType dataType;
        switch (type) {
            case DOUBLE:
                dataType = Types.DOUBLE;
                break;
            case FLOAT:
                dataType = Types.FLOAT;
                break;
            case INT64:
            case UINT64:
            case FIXED64:
            case SINT64:
            case SFIXED64:
                dataType = Types.BIGINT;
                break;
            case INT32:
            case UINT32:
            case FIXED32:
            case SINT32:
            case SFIXED32:
                dataType = Types.INT;
                break;
            case BOOL:
                dataType = Types.BOOLEAN;
                break;
            case STRING:
                dataType = Types.STRING;
                break;
            case BYTES:
                dataType = Types.BINARY;
                break;
            case ENUM:
                dataType = Types.INT;
                break;
            case MESSAGE:
                if (fd.isRepeated() && fd.getMessageType().getOptions().hasMapEntry()) {
                    throw new IllegalArgumentException(String.format("not supported type:%s(%s)", type, fd.getName()));
                } else {
                    StructField[] fields = fd.getMessageType().getFields().stream().map(f -> structFieldFor(f)).toArray(StructField[]::new);
                    dataType = new StructType(fields);
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("not supported type:%s(%s)", type, fd.getName()));
        }
        if (fd.isRepeated()) {
            return new StructField(fd.getName(), new ArrayType(dataType));
        } else {
            return new StructField(fd.getName(), dataType);
        }
    }

    public static class MessageConverter {
        FieldDesc[] fieldDescArray; // Message类型对应FieldDesc, 下标为field number

        public MessageConverter(Descriptor descriptor, StructType dataType) {
            List<FieldDescriptor> fields = descriptor.getFields();
            int maxNumber = fields.stream().mapToInt(f -> f.getNumber()).max().getAsInt();
            Preconditions.checkArgument(maxNumber < 10000, maxNumber);
            fieldDescArray = new FieldDesc[maxNumber + 1];
            for (FieldDescriptor field : fields) {
                Optional<StructField> structFieldOptional = Arrays.stream(dataType.fields).filter(f -> f.name.equals(field.getName())).findFirst();
                if(structFieldOptional.isPresent()){
                    fieldDescArray[field.getNumber()] = new FieldDesc(field, structFieldOptional.get().dataType);
                }
            }
        }

        public Map<String, Object> converter(CodedInputStream input) throws Exception {
            Map<String, Object> data = new HashMap<>();

            while (true) {
                int tag = input.readTag();
                if (tag == 0) {
                    break;
                }

                final int wireType = WireFormat.getTagWireType(tag);
                final int fieldNumber = WireFormat.getTagFieldNumber(tag);

                FieldDesc fieldDesc = null;
                if (fieldNumber < fieldDescArray.length) {
                    fieldDesc = fieldDescArray[fieldNumber];
                }

                boolean unknown = false;
                boolean packed = false;
                if (fieldDesc == null) {
                    unknown = true; // Unknown field.
                } else if (wireType == fieldDesc.field.getLiteType().getWireType()) {
                    packed = false;
                } else if (fieldDesc.field.isPackable() && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                    packed = true;
                } else {
                    unknown = true; // Unknown wire type.
                }

                if (unknown) { // Unknown field or wrong wire type.  Skip.
                    input.skipField(tag);
                    continue;
                }

                String name = fieldDesc.field.getName();
                if (packed) {
                    final int length = input.readRawVarint32();
                    final int limit = input.pushLimit(length);
                    List<Object> array = (List<Object>) fieldDesc.valueConverter.convert(input, true);
                    input.popLimit(limit);
                    List<Object> oldArray = (List<Object>)data.get(name);
                    if(oldArray == null){
                        data.put(name, array);
                    }else{
                        oldArray.addAll(array);
                    }
                } else {
                    final Object value = fieldDesc.valueConverter.convert(input, false);
                    if(!fieldDesc.field.isRepeated()){
                        data.put(name, value);
                    }else{
                        List<Object> array = (List<Object>)data.get(name);
                        if(array == null){
                            array = new ArrayList<>();
                            data.put(name, array);
                        }
                        array.add(value);
                    }
                }

            }

            return data;
        }
    }

    public static class FieldDesc {
        final FieldDescriptor field;
        final DataType fieldDataType; // field对应DataType，array类型存对应元素的类型

        final ValueConverter valueConverter;

        public FieldDesc(FieldDescriptor field, DataType dataType) {
            this.field = field;
            if (dataType instanceof ArrayType) {
                this.fieldDataType = ((ArrayType) dataType).elementType;
            } else {
                this.fieldDataType = dataType;
            }
            valueConverter = makeConverter();
        }

        private ValueConverter makeConverter() {
            switch (field.getType()) {
                case ENUM:
                    return (input, packed) -> {
                        if (packed) {
                            List<Object> array = new ArrayList<>();
                            while (input.getBytesUntilLimit() > 0) {
                                array.add(input.readEnum());
                            }
                            return array;
                        } else {
                            return input.readEnum();
                        }
                    };
                case MESSAGE:
                    final Descriptor descriptor = field.getMessageType();
                    final MessageConverter messageConverter = new MessageConverter(descriptor, (StructType) fieldDataType);
                    return (input, packed) -> {
                        final int length = input.readRawVarint32();
                        final int oldLimit = input.pushLimit(length);
                        Object message = messageConverter.converter(input);
                        input.checkLastTagWas(0);
                        if (input.getBytesUntilLimit() != 0) {
                            throw new RuntimeException("parse");
                        }
                        input.popLimit(oldLimit);
                        return message;
                    };
                default:
                    ValueConverter fieldConverter = makePrimitiveFieldConverter();
                    return (input, packed) -> {
                        if (packed) {
                            List<Object> array = new ArrayList<>();
                            while (input.getBytesUntilLimit() > 0) {
                                array.add(fieldConverter.convert(input, false));
                            }
                            return array;
                        } else {
                            return fieldConverter.convert(input, false);
                        }
                    };
            }
        }

        private ValueConverter makePrimitiveFieldConverter() {
            switch (field.getType()) {
                case DOUBLE:
                    if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> input.readDouble();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readDouble();
                    } else if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> (int) input.readDouble();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> (long) input.readDouble();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case FLOAT:
                    if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readFloat();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> input.readFloat();
                    } else if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> (int) input.readFloat();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> (long) input.readFloat();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case INT64:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> (int) input.readInt64();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> input.readInt64();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readInt64();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readInt64();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case UINT64:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> (int) input.readUInt64();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> input.readUInt64();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readUInt64();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readUInt64();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case FIXED64:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> (int) input.readFixed64();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> input.readFixed64();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readFixed64();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readFixed64();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case SFIXED64:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> (int) input.readSFixed64();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> input.readSFixed64();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readSFixed64();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readSFixed64();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case SINT64:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> (int) input.readSInt64();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> input.readSInt64();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readSInt64();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readSInt64();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case INT32:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> input.readInt32();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> (long) input.readInt32();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readInt32();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readInt32();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case UINT32:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> input.readUInt32();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> (long) input.readUInt32();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readUInt32();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readUInt32();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case FIXED32:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> input.readFixed32();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> (long) input.readFixed32();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readFixed32();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readFixed32();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case SFIXED32:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> input.readSFixed32();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> (long) input.readSFixed32();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readSFixed32();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readSFixed32();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case SINT32:
                    if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> input.readSInt32();
                    } else if (fieldDataType instanceof LongType) {
                        return (input, packed) -> (long) input.readSInt32();
                    } else if (fieldDataType instanceof FloatType) {
                        return (input, packed) -> (float) input.readSInt32();
                    } else if (fieldDataType instanceof DoubleType) {
                        return (input, packed) -> (double) input.readSInt32();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case BOOL:
                    if (fieldDataType instanceof BooleanType) {
                        return (input, packed) -> input.readBool();
                    } else if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> input.readBool() ? 1 : 0;
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case BYTES:
                    if (fieldDataType instanceof BinaryType) {
                        return (input, packed) -> input.readByteArray();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                case STRING:
                    if (fieldDataType instanceof StringType) {
                        return (input, packed) -> input.readString();
                    } else {
                        throw new IllegalArgumentException(String.format("type:%s can not convert to type:%s", field.getType(), fieldDataType.simpleString()));
                    }
                default:
                    throw new IllegalArgumentException(String.format("not supported type:%s(%s)", field.getType(), field.getName()));
            }
        }

    }

    @FunctionalInterface
    public interface ValueConverter {
        Object convert(CodedInputStream input, boolean packed) throws Exception;
    }
}
