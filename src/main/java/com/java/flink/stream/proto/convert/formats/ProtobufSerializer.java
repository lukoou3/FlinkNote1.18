package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class ProtobufSerializer {
    final MessageData messageData;

    public ProtobufSerializer(Descriptor descriptor) {
        messageData = new MessageData(descriptor);
    }

    public byte[] serialize(Map<String, Object> data) throws Exception {
        messageData.feed(data);
        byte[] result = new byte[messageData.getSerializedSize()];
        CodedOutputStream output = CodedOutputStream.newInstance(result);
        messageData.writeTo(output);
        output.checkNoSpaceLeft();
        return result;
    }

    static double convertToDouble(Object obj) throws Exception {
        if (obj instanceof Double) {
            return (Double) obj;
        } else if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        } else {
            throw new IllegalArgumentException("can not convert to double");
        }
    }

    static float convertToFloat(Object obj) throws Exception {
        if (obj instanceof Float) {
            return (Float) obj;
        } else if (obj instanceof Number) {
            return ((Number) obj).floatValue();
        } else if (obj instanceof String) {
            return Float.parseFloat((String) obj);
        } else {
            throw new IllegalArgumentException("can not convert to double");
        }
    }

    static int convertToInt(Object obj) throws Exception {
        if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Number) {
            return ((Number) obj).intValue();
        } else if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        } else {
            throw new IllegalArgumentException("can not convert to double");
        }
    }

    static long convertToLong(Object obj) throws Exception {
        if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof Number) {
            return ((Number) obj).longValue();
        } else if (obj instanceof String) {
            return Long.parseLong((String) obj);
        } else {
            throw new IllegalArgumentException("can not convert to long:" + obj);
        }
    }

    static boolean convertToBool(Object obj) throws Exception {
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof Number) {
            return ((Number) obj).intValue() != 0;
        } else {
            throw new IllegalArgumentException("can not convert to double");
        }
    }

    static class MessageData extends ProtobufData {
        final FieldData[] fieldDatas;
        final Map<String, FieldData> fieldDataMap;

        public MessageData(Descriptor descriptor) {
            List<FieldDescriptor> fields = descriptor.getFields();
            fieldDatas = new FieldData[fields.size()];
            fieldDataMap = new HashMap<>();
            for (int i = 0; i < fields.size(); i++) {
                fieldDatas[i] = FieldData.newInstance(fields.get(i));
                fieldDataMap.put(fieldDatas[i].name, fieldDatas[i]);
            }
        }

        @Override
        public int getSerializedSize() {
            int size = 0;
            FieldData fieldData;
            for (int i = 0; i < fieldDatas.length; i++) {
                fieldData = fieldDatas[i];
                if (!fieldData.isNull()) {
                    size += fieldData.getSerializedSize();
                    System.out.println(fieldData.name + " -> " + size);
                }
            }
            return size;
        }

        @Override
        public void feed(Object obj) throws Exception {
            Map<String, Object> map = (Map<String, Object>) obj;
            FieldData fieldData;
            for (int i = 0; i < fieldDatas.length; i++) {
                fieldData = fieldDatas[i];
                fieldData.reset();
                Object value = map.get(fieldData.name);
                if (value != null) {
                    fieldData.feed(value);
                }
            }
        }

        @Override
        public void writeTo(CodedOutputStream output) throws Exception {
            FieldData fieldData;
            for (int i = 0; i < fieldDatas.length; i++) {
                fieldData = fieldDatas[i];
                if (!fieldData.isNull()) {
                    fieldData.writeTo(output);
                    System.out.println(fieldData.name + " -> " + output.spaceLeft());
                }
            }
        }
    }

    static abstract class FieldData {
        final int tag;
        final int tagSize;
        final FieldDescriptor field;

        final String name;
        final boolean message;
        final boolean packed;
        final boolean repeated;

        FieldData(int tag, int tagSize, FieldDescriptor field) {
            this.tag = tag;
            this.tagSize = tagSize;
            this.field = field;
            this.name = field.getName();
            this.message = field.getType() == FieldDescriptor.Type.MESSAGE;
            this.packed = field.isPacked();
            this.repeated = field.isRepeated();
        }

        abstract void reset();

        abstract boolean isNull();

        abstract int getSerializedSize();

        abstract void feed(Object obj) throws Exception;

        abstract void writeTo(CodedOutputStream output) throws Exception;

        static FieldData newInstance(FieldDescriptor field) {
            WireFormat.FieldType type = field.getLiteType();
            int tag = makeTag(field.getNumber(), getWireFormatForFieldType(type, field.isPacked()));
            int tagSize = CodedOutputStream.computeTagSize(field.getNumber());
            Supplier<ProtobufData> dataSupplier = getProtobufDataSupplier(field);
            if (field.isRepeated()) {
                return new ArrayFieldData(tag, tagSize, field, dataSupplier);
            } else {
                return new ValueFieldData(tag, tagSize, field, dataSupplier.get());
            }
        }

        static Supplier<ProtobufData> getProtobufDataSupplier(FieldDescriptor field) {
            switch (field.getLiteType()) {
                case DOUBLE:
                    return () -> new DoubleData();
                case FLOAT:
                    return () -> new FloatData();
                case INT64:
                    return () -> new Int64Data();
                case ENUM:
                case INT32:
                    return () -> new Int32Data();
                case STRING:
                    return () -> new StringData();
                case BYTES:
                    return () -> new BytesData();
                case BOOL:
                    return () -> new BoolData();
                case MESSAGE:
                    return () -> new MessageData(field.getMessageType());
                default:
                    throw new IllegalArgumentException(String.format("not supported type:%s(%s)", field.getType(), field.getName()));
            }
        }
    }

    static class ValueFieldData extends FieldData {
        final ProtobufData data;
        boolean na = true;

        ValueFieldData(int tag, int tagSize, FieldDescriptor field, ProtobufData data) {
            super(tag, tagSize, field);
            this.data = data;
        }

        @Override
        void reset() {
            na = true;
        }

        @Override
        boolean isNull() {
            return na;
        }

        @Override
        int getSerializedSize() {
            if (message) {
                return tagSize + computeLengthDelimitedFieldSize(data.getSerializedSize());
            } else {
                return tagSize + data.getSerializedSize();
            }
        }

        @Override
        void feed(Object obj) throws Exception {
            data.feed(obj);
            na = false;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            // com.google.protobuf.FieldSet.writeElement
            output.writeUInt32NoTag(tag);
            if (message) {
                output.writeUInt32NoTag(data.getSerializedSize());
            }
            data.writeTo(output);
        }
    }

    static class ArrayFieldData extends FieldData {
        final List<ProtobufData> datas;
        private int pos = 0;
        int dataSize = 0;
        final Supplier<ProtobufData> dataSupplier;

        ArrayFieldData(int tag, int tagSize, FieldDescriptor field, Supplier<ProtobufData> dataSupplier) {
            super(tag, tagSize, field);
            this.datas = new ArrayList<>();
            this.dataSupplier = dataSupplier;
        }

        @Override
        void reset() {
            pos = 0;
        }

        @Override
        boolean isNull() {
            return pos == 0;
        }

        @Override
        int getSerializedSize() {
            // com.google.protobuf.FieldSet.computeFieldSize
            if (packed) {
                int size = 0;
                for (int i = 0; i < pos; i++) {
                    size += message ? computeLengthDelimitedFieldSize(datas.get(i).getSerializedSize()) : datas.get(i).getSerializedSize();
                }
                dataSize = size;
                size += CodedOutputStream.computeUInt32SizeNoTag(size);
                return tagSize + size;
            } else {
                int size = 0;
                for (int i = 0; i < pos; i++) {
                    size += tagSize;
                    size += message ? computeLengthDelimitedFieldSize(datas.get(i).getSerializedSize()) : datas.get(i).getSerializedSize();
                }
                return size;
            }
        }

        @Override
        void feed(Object obj) throws Exception {
            List<Object> list = (List<Object>) obj;
            ProtobufData data;
            if(datas.size() < list.size()){
                int len = list.size() - datas.size();
                for (int i = 0; i < len; i++) {
                    datas.add(dataSupplier.get());
                }
            }
            for (int i = 0; i < list.size(); i++) {
                data = datas.get(i);
                data.feed(list.get(i));
            }
            pos = list.size();
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            //com.google.protobuf.FieldSet.writeField
            if (packed) {
                output.writeUInt32NoTag(tag);
                output.writeUInt32NoTag(dataSize);
                for (int i = 0; i < pos; i++) {
                    datas.get(i).writeTo(output);
                }
            } else {
                for (int i = 0; i < pos; i++) {
                    output.writeUInt32NoTag(tag);
                    if(message){
                        output.writeUInt32NoTag(datas.get(i).getSerializedSize());
                    }
                    datas.get(i).writeTo(output);
                }
            }
        }
    }

    static class DoubleData extends ProtobufData {
        private double value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeDoubleSizeNoTag(value); // FIXED64_SIZE
        }

        @Override
        void feed(Object obj) throws Exception {
            value = convertToDouble(obj);
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeDoubleNoTag(value);
        }
    }

    static class FloatData extends ProtobufData {
        private float value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeFloatSizeNoTag(value); // FIXED32_SIZE
        }

        @Override
        void feed(Object obj) throws Exception {
            value = convertToFloat(obj);
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeFloatNoTag(value);
        }
    }


    static class Int32Data extends ProtobufData {
        private int value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeInt32SizeNoTag(value);
        }

        @Override
        void feed(Object obj) throws Exception {
            value = convertToInt(obj);
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeInt32NoTag(value);
        }
    }

    static class Int64Data extends ProtobufData {
        private long value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeInt64SizeNoTag(value);
        }

        @Override
        void feed(Object obj) throws Exception {
            value = convertToLong(obj);
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeInt64NoTag(value);
        }
    }

    static class StringData extends ProtobufData {
        private byte[] value;

        @Override
        int getSerializedSize() {
            return computeLengthDelimitedFieldSize(value.length);
        }

        @Override
        void feed(Object obj) throws Exception {
            value = obj.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeByteArrayNoTag(value);
        }
    }

    static class BytesData extends ProtobufData {
        private byte[] value;

        @Override
        int getSerializedSize() {
            return computeLengthDelimitedFieldSize(value.length);
        }

        @Override
        void feed(Object obj) throws Exception {
            value = (byte[]) obj;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeByteArrayNoTag(value);
        }
    }

    static class BoolData extends ProtobufData {
        private boolean value;

        @Override
        int getSerializedSize() {
            return 1;
        }

        @Override
        void feed(Object obj) throws Exception {
            value = convertToBool(obj);
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeBoolNoTag(value);
        }
    }


    static abstract class ProtobufData {
        abstract int getSerializedSize();

        abstract void feed(Object obj) throws Exception;

        abstract void writeTo(CodedOutputStream output) throws Exception;
    }

    static final int TAG_TYPE_BITS = 3;

    static int makeTag(final int fieldNumber, final int wireType) {
        return (fieldNumber << TAG_TYPE_BITS) | wireType;
    }

    static int getWireFormatForFieldType(final WireFormat.FieldType type, boolean isPacked) {
        if (isPacked) {
            return WireFormat.WIRETYPE_LENGTH_DELIMITED;
        } else {
            return type.getWireType();
        }
    }

    static int computeLengthDelimitedFieldSize(int fieldLength) {
        return CodedOutputStream.computeUInt32SizeNoTag(fieldLength) + fieldLength;
    }
}
