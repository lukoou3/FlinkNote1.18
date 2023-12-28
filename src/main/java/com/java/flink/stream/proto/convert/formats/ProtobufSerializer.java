package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ProtobufSerializer {
    final MessageData messageData;

    public ProtobufSerializer(Descriptor descriptor) {
        ProtobufUtils.checkSupportParseDescriptor(descriptor);
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
        int memoizedSize = -1;

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
            if(memoizedSize != -1){
                return memoizedSize;
            }

            int size = 0;
            FieldData fieldData;
            for (int i = 0; i < fieldDatas.length; i++) {
                fieldData = fieldDatas[i];
                if (fieldData.isNotNull()) {
                    size += fieldData.getSerializedSize();
                    //System.out.println(fieldData.name + " -> " + size);
                }
            }

            memoizedSize = size;
            return size;
        }

        @Override
        public boolean feed(Object obj) throws Exception {
            memoizedSize = -1;
            Map<String, Object> map = (Map<String, Object>) obj;
            FieldData fieldData;
            for (int i = 0; i < fieldDatas.length; i++) {
                fieldData = fieldDatas[i];
                fieldData.reset();
            }
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                fieldData = fieldDataMap.get(entry.getKey());
                if(fieldData != null) {
                    Object value = entry.getValue();
                    if (value != null) {
                        fieldData.feed(value);
                    }
                }
            }
            return true;
        }

        /*@Override
        public boolean feed(Object obj) throws Exception {
            memoizedSize = -1;
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
            return true;
        }*/

        @Override
        public void writeTo(CodedOutputStream output) throws Exception {
            FieldData fieldData;
            for (int i = 0; i < fieldDatas.length; i++) {
                fieldData = fieldDatas[i];
                if (fieldData.isNotNull()) {
                    fieldData.writeTo(output);
                    //System.out.println(fieldData.name + " -> " + output.spaceLeft());
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
        final boolean optional;
        final boolean packed;
        final boolean repeated;

        FieldData(int tag, int tagSize, FieldDescriptor field) {
            this.tag = tag;
            this.tagSize = tagSize;
            this.field = field;
            this.name = field.getName();
            this.message = field.getType() == FieldDescriptor.Type.MESSAGE;
            this.optional = field.hasOptionalKeyword();
            this.packed = field.isPacked();
            this.repeated = field.isRepeated();
        }

        abstract void reset();

        abstract boolean isNotNull();

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
                case UINT64:
                    return () -> new Uint64Data();
                case FIXED64:
                    return () -> new Fixed64Data();
                case SFIXED64:
                    return () -> new SFixed64Data();
                case SINT64:
                    return () -> new SInt64Data();
                case INT32:
                    return () -> new Int32Data();
                case UINT32:
                    return () -> new Uint32Data();
                case FIXED32:
                    return () -> new Fixed32Data();
                case SFIXED32:
                    return () -> new SFixed32Data();
                case SINT32:
                    return () -> new SInt32Data();
                case STRING:
                    return () -> new StringData();
                case BYTES:
                    return () -> new BytesData();
                case BOOL:
                    return () -> new BoolData();
                case MESSAGE:
                    return () -> new MessageData(field.getMessageType());
                case ENUM:
                    int number = ((EnumValueDescriptor) field.getDefaultValue()).getNumber();
                    return () -> new EnumData(number);
                default:
                    throw new IllegalArgumentException(String.format("not supported type:%s(%s)", field.getType(), field.getName()));
            }
        }
    }

    static class ValueFieldData extends FieldData {
        final ProtobufData data;
        boolean notNull = false;

        ValueFieldData(int tag, int tagSize, FieldDescriptor field, ProtobufData data) {
            super(tag, tagSize, field);
            this.data = data;
        }

        @Override
        void reset() {
            notNull = false;
        }

        @Override
        boolean isNotNull() {
            return notNull;
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
            notNull = data.feed(obj);
            if(optional){
                notNull = true;
            }
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
        boolean isNotNull() {
            return pos != 0;
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
        boolean feed(Object obj) throws Exception {
            value = convertToDouble(obj);
            return value != 0D;
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
        boolean feed(Object obj) throws Exception {
            value = convertToFloat(obj);
            return value != 0F;
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
        boolean feed(Object obj) throws Exception {
            value = convertToInt(obj);
            return value != 0;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeInt32NoTag(value);
        }
    }

    static class Uint32Data extends ProtobufData {
        private int value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeInt32SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToInt(obj);
            return value != 0;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeUInt32NoTag(value);
        }
    }

    static class Fixed32Data extends ProtobufData {
        private int value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeFixed32SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToInt(obj);
            return value != 0;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeFixed32NoTag(value);
        }
    }

    static class SFixed32Data extends ProtobufData {
        private int value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeSFixed32SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToInt(obj);
            return value != 0;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeSFixed32NoTag(value);
        }
    }

    static class SInt32Data extends ProtobufData {
        private int value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeSInt32SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToInt(obj);
            return value != 0;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeSInt32NoTag(value);
        }
    }

    static class Int64Data extends ProtobufData {
        private long value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeInt64SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToLong(obj);
            return value != 0L;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeInt64NoTag(value);
        }
    }

    static class Uint64Data extends ProtobufData {
        private long value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeInt64SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToLong(obj);
            return value != 0L;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeUInt64NoTag(value);
        }
    }

    static class Fixed64Data extends ProtobufData {
        private long value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeFixed64SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToLong(obj);
            return value != 0L;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeFixed64NoTag(value);
        }
    }

    static class SFixed64Data extends ProtobufData {
        private long value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeSFixed64SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToLong(obj);
            return value != 0L;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeSFixed64NoTag(value);
        }
    }

    static class SInt64Data extends ProtobufData {
        private long value;

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeSInt64SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToLong(obj);
            return value != 0L;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeSInt64NoTag(value);
        }
    }

    static final int MAX_STR_BYTES_LENGTH = 1024 * 16;
    static class StringData extends ProtobufData {
        private final byte[] bytes = new byte[MAX_STR_BYTES_LENGTH];
        private byte[] value;
        private int len;

        @Override
        int getSerializedSize() {
            return computeLengthDelimitedFieldSize(len);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            String str = obj.toString();
            if(str.isEmpty()){
                value = bytes;
                len = 0;
                return false;
            }
            //value = str.getBytes(StandardCharsets.UTF_8);
            int length = str.length() * 3;
            if (length > MAX_STR_BYTES_LENGTH) {
                value = new byte[length];
            }else{
                value = bytes;
            }

            len = encodeUTF8(str, value);
            return true;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeUInt32NoTag(len);
            output.write(value, 0, len);
        }
    }

    static class BytesData extends ProtobufData {
        private byte[] value;

        @Override
        int getSerializedSize() {
            return computeLengthDelimitedFieldSize(value.length);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = (byte[]) obj;
            return value.length != 0;
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
        boolean feed(Object obj) throws Exception {
            value = convertToBool(obj);
            return value;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeBoolNoTag(value);
        }
    }

    static class EnumData extends ProtobufData {
        private int value;
        final private int defaultValue;

        public EnumData(int defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        int getSerializedSize() {
            return CodedOutputStream.computeInt32SizeNoTag(value);
        }

        @Override
        boolean feed(Object obj) throws Exception {
            value = convertToInt(obj);
            return value != defaultValue;
        }

        @Override
        void writeTo(CodedOutputStream output) throws Exception {
            output.writeInt32NoTag(value);
        }
    }

    static abstract class ProtobufData {
        abstract int getSerializedSize();

        abstract boolean feed(Object obj) throws Exception;

        abstract void writeTo(CodedOutputStream output) throws Exception;
    }

    // copy from org.apache.flink.table.runtime.util.StringUtf8Utils
    static int encodeUTF8(String str, byte[] bytes) {
        int offset = 0;
        int len = str.length();
        int sl = offset + len;
        int dp = 0;
        int dlASCII = dp + Math.min(len, bytes.length);

        // ASCII only optimized loop
        while (dp < dlASCII && str.charAt(offset) < '\u0080') {
            bytes[dp++] = (byte) str.charAt(offset++);
        }

        while (offset < sl) {
            char c = str.charAt(offset++);
            if (c < 0x80) {
                // Have at most seven bits
                bytes[dp++] = (byte) c;
            } else if (c < 0x800) {
                // 2 bytes, 11 bits
                bytes[dp++] = (byte) (0xc0 | (c >> 6));
                bytes[dp++] = (byte) (0x80 | (c & 0x3f));
            } else if (Character.isSurrogate(c)) {
                final int uc;
                int ip = offset - 1;
                if (Character.isHighSurrogate(c)) {
                    if (sl - ip < 2) {
                        uc = -1;
                    } else {
                        char d = str.charAt(ip + 1);
                        if (Character.isLowSurrogate(d)) {
                            uc = Character.toCodePoint(c, d);
                        } else {
                            // for some illegal character
                            // the jdk will ignore the origin character and cast it to '?'
                            // this acts the same with jdk
                            return defaultEncodeUTF8(str, bytes);
                        }
                    }
                } else {
                    if (Character.isLowSurrogate(c)) {
                        // for some illegal character
                        // the jdk will ignore the origin character and cast it to '?'
                        // this acts the same with jdk
                        return defaultEncodeUTF8(str, bytes);
                    } else {
                        uc = c;
                    }
                }

                if (uc < 0) {
                    bytes[dp++] = (byte) '?';
                } else {
                    bytes[dp++] = (byte) (0xf0 | ((uc >> 18)));
                    bytes[dp++] = (byte) (0x80 | ((uc >> 12) & 0x3f));
                    bytes[dp++] = (byte) (0x80 | ((uc >> 6) & 0x3f));
                    bytes[dp++] = (byte) (0x80 | (uc & 0x3f));
                    offset++; // 2 chars
                }
            } else {
                // 3 bytes, 16 bits
                bytes[dp++] = (byte) (0xe0 | ((c >> 12)));
                bytes[dp++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                bytes[dp++] = (byte) (0x80 | (c & 0x3f));
            }
        }
        return dp;
    }

    static int defaultEncodeUTF8(String str, byte[] bytes) {
        try {
            byte[] buffer = str.getBytes("UTF-8");
            System.arraycopy(buffer, 0, bytes, 0, buffer.length);
            return buffer.length;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("encodeUTF8 error", e);
        }
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
