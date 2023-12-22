package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.Descriptors;
import com.java.flink.stream.proto.convert.types.*;
import com.java.flink.stream.proto.convert.types.StructType.StructField;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.runtime.util.StringUtf8Utils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SchemaConverters {
    static final Logger LOG = LoggerFactory.getLogger(SchemaConverters.class);
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

    // 校验dataType和descriptor是否匹配，dataType中定义的属性必须全部在descriptor定义，每个字段的类型必须匹配(相同或者能够转换)
    public static void checkMatch(Descriptor descriptor, StructType dataType) throws Exception {
        checkMatch(descriptor, dataType, null);
    }

    private static void checkMatch(Descriptor descriptor, StructType dataType, String prefix) throws Exception {
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        Map<String, FieldDescriptor> fdMap = fieldDescriptors.stream().collect(Collectors.toMap(x -> x.getName(), x -> x));
        StructField[] fields = dataType.fields;

        for (int i = 0; i < fields.length; i++) {
            StructField field = fields[i];
            FieldDescriptor fd = fdMap.get(field.name);
            if(fd == null){
                throw new IllegalArgumentException(String.format("%s ' field:%s not found in proto descriptor", StringUtils.isBlank(prefix)? "root": prefix, field));
            }
            WireFormat.FieldType type = fd.getLiteType();
            DataType fieldDataType;
            if(fd.isRepeated()){
                if(!(field.dataType instanceof ArrayType)){
                    throw newNotMatchException(field, fd, prefix);
                }
                fieldDataType = ((ArrayType)field.dataType).elementType;
            }else{
                fieldDataType = field.dataType;
            }
            switch (type) {
                case DOUBLE:
                case FLOAT:
                    if(!(fieldDataType instanceof DoubleType || fieldDataType instanceof FloatType
                            || fieldDataType instanceof IntegerType || fieldDataType instanceof LongType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    break;
                case INT64:
                case UINT64:
                case FIXED64:
                case SINT64:
                case SFIXED64:
                    if(!(fieldDataType instanceof IntegerType || fieldDataType instanceof LongType
                            || fieldDataType instanceof FloatType || fieldDataType instanceof DoubleType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    break;
                case INT32:
                case UINT32:
                case FIXED32:
                case SINT32:
                case SFIXED32:
                    if(!(fieldDataType instanceof IntegerType || fieldDataType instanceof LongType
                            || fieldDataType instanceof FloatType || fieldDataType instanceof DoubleType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    break;
                case BOOL:
                    if(!(fieldDataType instanceof BooleanType || fieldDataType instanceof IntegerType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    break;
                case STRING:
                    if(!(fieldDataType instanceof StringType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    break;
                case BYTES:
                    if(!(fieldDataType instanceof BinaryType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    break;
                case ENUM:
                    if(!(fieldDataType instanceof IntegerType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    break;
                case MESSAGE:
                    if(!(fieldDataType instanceof StructType)){
                        throw newNotMatchException(field, fd, prefix);
                    }
                    checkMatch(fd.getMessageType(), (StructType) fieldDataType, StringUtils.isBlank(prefix)? field.name: prefix + "." + field.name);
            }
        }
    }

    private static IllegalArgumentException newNotMatchException(StructField field, FieldDescriptor fd, String prefix){
        return new IllegalArgumentException(String.format("%s ' field:%s not match with proto field descriptor:%s(%s)", StringUtils.isBlank(prefix)? "root": prefix, field, fd, fd.getType()));
    }

    public static class MessageConverter {
        FieldDesc[] fieldDescArray; // Message类型对应FieldDesc, 下标为field number
        int initialCapacity = 0;
        final boolean emitDefaultValues;
        final DefaultValue[] defaultValues;

        MessageConverter(Descriptor descriptor, StructType dataType) {
            this(descriptor, dataType, false);
        }

        public MessageConverter(Descriptor descriptor, StructType dataType, boolean emitDefaultValues) {
            List<FieldDescriptor> fields = descriptor.getFields();
            int maxNumber = fields.stream().mapToInt(f -> f.getNumber()).max().getAsInt();
            Preconditions.checkArgument(maxNumber < 10000, maxNumber);
            fieldDescArray = new FieldDesc[maxNumber + 1];

            this.emitDefaultValues = emitDefaultValues;
            if(this.emitDefaultValues){
                defaultValues = new DefaultValue[dataType.fields.length];
            }else{
                defaultValues = null;
            }

            for (FieldDescriptor field : fields) {
                // Optional<StructField> structFieldOptional = Arrays.stream(dataType.fields).filter(f -> f.name.equals(field.getName())).findFirst();
                // if(structFieldOptional.isPresent()){
                int position = -1;
                for (int i = 0; i < dataType.fields.length; i++) {
                    if(dataType.fields[i].name.equals(field.getName())){
                        position = i;
                        break;
                    }
                }
                if(position >= 0){
                    fieldDescArray[field.getNumber()] = new FieldDesc(field, dataType.fields[position].dataType, position, emitDefaultValues);
                    if(this.emitDefaultValues){
                        defaultValues[position] = new DefaultValue(dataType.fields[position].name, getDefaultValue(field, dataType.fields[position].dataType));
                    }
                }
            }
            if(dataType.fields.length / 3 > 16){
                initialCapacity = (dataType.fields.length / 3) ;
            }
            if(this.emitDefaultValues){
                for (int i = 0; i < defaultValues.length; i++) {
                    if (defaultValues[i] == null) {
                        throw new IllegalArgumentException(String.format("%s and %s not match", dataType, descriptor));
                    }
                }
            }
        }

        public Map<String, Object> converter(CodedInputStream input) throws Exception {
            Map<String, Object> data = initialCapacity == 0? new HashMap<>(): new HashMap<>(initialCapacity);

            if(emitDefaultValues){
                for (int i = 0; i < defaultValues.length; i++) {
                    defaultValues[i].hasValue = false;
                }
            }

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

                if(emitDefaultValues){
                    defaultValues[fieldDesc.fieldPosition].hasValue = true;
                }

                String name = fieldDesc.name;
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

            if(emitDefaultValues){
                DefaultValue defaultValue;
                for (int i = 0; i < defaultValues.length; i++) {
                    defaultValue = defaultValues[i];
                    if(!defaultValue.hasValue && defaultValue.defaultValue != null){
                        data.put(defaultValue.name, defaultValue.defaultValue);
                    }
                }
            }

            return data;
        }

        private Object getDefaultValue(FieldDescriptor field, DataType fieldDataType){
            if(field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE){
                return null;
            }
            if(field.isRepeated()){
                return null;
            }
            if(field.hasOptionalKeyword()){
                return null;
            }

            switch (field.getType()) {
                case DOUBLE:
                case FLOAT:
                case INT64:
                case UINT64:
                case FIXED64:
                case SFIXED64:
                case SINT64:
                case INT32:
                case UINT32:
                case FIXED32:
                case SFIXED32:
                case SINT32:
                    Number number = 0L;
                    if (fieldDataType instanceof DoubleType) {
                        return number.doubleValue();
                    } else if (fieldDataType instanceof FloatType) {
                        return number.floatValue();
                    } else if (fieldDataType instanceof IntegerType) {
                        return number.intValue();
                    } else if (fieldDataType instanceof LongType) {
                        return number.longValue();
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                case BOOL:
                    if (fieldDataType instanceof BooleanType) {
                        return false;
                    } else if (fieldDataType instanceof IntegerType) {
                        return 0;
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                case BYTES:
                    if (fieldDataType instanceof BinaryType) {
                        return new byte[0];
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                case STRING:
                    if (fieldDataType instanceof StringType) {
                        return "";
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                case ENUM:
                    if (fieldDataType instanceof IntegerType) {
                        return ((Descriptors.EnumValueDescriptor) field.getDefaultValue()).getNumber();
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                default:
                    throw new IllegalArgumentException(String.format("not supported proto type:%s(%s)", field.getType(), field.getName()));
            }
        }
    }

    public static class DefaultValue{
        boolean hasValue;
        final String name;

        final Object defaultValue;

        public DefaultValue(String name, Object defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }
    }

    public static class FieldDesc {
        final FieldDescriptor field;
        final String name;
        final DataType fieldDataType; // field对应DataType，array类型存对应元素的类型
        final int fieldPosition; // field位置

        final ValueConverter valueConverter;

        public FieldDesc(FieldDescriptor field, DataType dataType, int fieldPosition, boolean emitDefaultValues) {
            this.field = field;
            this.name = field.getName();
            if (dataType instanceof ArrayType) {
                this.fieldDataType = ((ArrayType) dataType).elementType;
            } else {
                this.fieldDataType = dataType;
            }
            this.fieldPosition = fieldPosition;
            valueConverter = makeConverter(emitDefaultValues);
        }

        private ValueConverter makeConverter(boolean emitDefaultValues) {
            switch (field.getType()) {
                case ENUM:
                    if(!(fieldDataType instanceof IntegerType)){
                        throw newCanNotConvertException(field, fieldDataType);
                    }
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
                    final MessageConverter messageConverter = new MessageConverter(descriptor, (StructType) fieldDataType, emitDefaultValues);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
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
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                case BOOL:
                    if (fieldDataType instanceof BooleanType) {
                        return (input, packed) -> input.readBool();
                    } else if (fieldDataType instanceof IntegerType) {
                        return (input, packed) -> input.readBool() ? 1 : 0;
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                case BYTES:
                    if (fieldDataType instanceof BinaryType) {
                        return (input, packed) -> input.readByteArray();
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                case STRING:
                    if (fieldDataType instanceof StringType) {
                        return (input, packed) -> {
                            //return input.readString();
                            byte[] bytes = input.readByteArray();
                            return StringUtf8Utils.decodeUTF8(bytes, 0, bytes.length);
                        };
                    } else {
                        throw newCanNotConvertException(field, fieldDataType);
                    }
                default:
                    throw new IllegalArgumentException(String.format("not supported proto type:%s(%s)", field.getType(), field.getName()));
            }
        }

    }

    private static IllegalArgumentException newCanNotConvertException(FieldDescriptor field, DataType fieldDataType){
        return new IllegalArgumentException(String.format("proto field:%s(%s) can not convert to type:%s", field.getName(), field.getType(), fieldDataType.simpleString()));
    }

    @FunctionalInterface
    public interface ValueConverter {
        Object convert(CodedInputStream input, boolean packed) throws Exception;
    }
}
