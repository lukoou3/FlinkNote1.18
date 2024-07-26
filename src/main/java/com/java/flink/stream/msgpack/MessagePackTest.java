package com.java.flink.stream.msgpack;

import org.junit.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableMapValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.ValueType;

public class MessagePackTest {

    @Test
    public void testSer() throws Exception{
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(2);
        packer.packString("id").packInt(123);
        packer.packString("name").packString("abc");
        byte[] serializeData = packer.toByteArray();
        packer.close();
        System.out.println(serializeData);
    }

    @Test
    public void testSer2() throws Exception{
        ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();
        map.put(ValueFactory.newString("id"), ValueFactory.newInteger(123));
        map.put(ValueFactory.newString("name"), ValueFactory.newString("abc"));
        MapValue mapValue = map.build();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packValue(mapValue);
        byte[] serializeData = packer.toByteArray();
        packer.close();
        System.out.println(serializeData);
    }

    @Test
    public void testDeser() throws Exception{
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(2);
        packer.packString("id").packInt(123);
        packer.packString("name").packString("abc");
        byte[] serializeData = packer.toByteArray();
        packer.close();
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(serializeData);
        ValueType valueType = unpacker.getNextFormat().getValueType();
        assert valueType == ValueType.MAP;
        ImmutableMapValue mapValue = unpacker.unpackValue().asMapValue();
        System.out.println(mapValue);
    }

    @Test
    public void testValueTypeIndex() throws Exception{
        ValueType[] values = ValueType.values();
        for (int i = 0; i < values.length; i++) {
            ValueType value = values[i];
            System.out.println(value.ordinal() + ":" + value.name());
        }
    }
}
