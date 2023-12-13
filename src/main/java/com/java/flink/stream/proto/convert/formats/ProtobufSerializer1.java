package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;

import java.util.List;

public class ProtobufSerializer1 {

    public ProtobufSerializer1(Descriptor descriptor) {

    }

    public static class MessageDataWriter implements DataWriter {
        private boolean isNull = true;

        public MessageDataWriter(Descriptor descriptor) {
            List<FieldDescriptor> fields = descriptor.getFields();

        }

        @Override
        public int getSerializedSize() {
            // 属性：tagSize + computeElementSizeNoTag(type, value);
            return 0;
        }

        @Override
        public void feed(Object obj) throws Exception{

        }

        @Override
        public boolean dataIsNull() {
            return isNull;
        }

        @Override
        public void write(CodedOutputStream output) throws Exception {

        }
    }

    private static class FieldWriter {
        final int tag; // (fieldNumber << TAG_TYPE_BITS) | wireType.  writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        final int tagSize;
        final FieldDescriptor field;
        final DataWriter dataWriter;

        public FieldWriter(FieldDescriptor field) {
            this.field = field;
            WireFormat.FieldType type = field.getLiteType();
            this.tag = makeTag(field.getNumber(), getWireFormatForFieldType(type, field.isPacked()));
            this.tagSize = CodedOutputStream.computeTagSize(field.getNumber());
            this.dataWriter = null;

        }
    }

    private static class DoubleWriter implements DataWriter {
        private boolean isNull = true;
        private double value;

        @Override
        public int getSerializedSize() {
            return 8; // FIXED64_SIZE
        }

        @Override
        public void feed(Object obj) throws Exception {
            if(obj instanceof Double) {
                value = (Double) obj;
            } else if (obj instanceof Number) {
                value = ((Number) obj).doubleValue();
            }else if (obj instanceof String) {
                value = Double.parseDouble((String) obj);
            }else{
                throw new IllegalArgumentException("can not convert to double");
            }
        }

        @Override
        public boolean dataIsNull() {
            return isNull;
        }

        @Override
        public void write(CodedOutputStream output) throws Exception {

        }
    }

    public interface DataWriter {
        int getSerializedSize();

        void feed(Object obj) throws Exception;

        boolean dataIsNull();

        void write(CodedOutputStream output) throws Exception;
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
}
