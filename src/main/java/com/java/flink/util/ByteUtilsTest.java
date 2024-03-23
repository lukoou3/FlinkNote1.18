package com.java.flink.util;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ByteUtilsTest {

    private final byte x00 = 0x00;
    private final byte x01 = 0x01;
    private final byte x02 = 0x02;
    private final byte x0F = 0x0f;
    private final byte x07 = 0x07;
    private final byte x08 = 0x08;
    private final byte x3F = 0x3f;
    private final byte x40 = 0x40;
    private final byte x7E = 0x7E;
    private final byte x7F = 0x7F;
    private final byte xFF = (byte) 0xff;
    private final byte x80 = (byte) 0x80;
    private final byte x81 = (byte) 0x81;
    private final byte xBF = (byte) 0xbf;
    private final byte xC0 = (byte) 0xc0;
    private final byte xFE = (byte) 0xfe;

    @Test
    public void testReadUnsignedIntLEFromArray() {
        byte[] array1 = {0x01, 0x02, 0x03, 0x04, 0x05};
        assertEquals(0x04030201, ByteUtils.readUnsignedIntLE(array1, 0));
        assertEquals(0x05040302, ByteUtils.readUnsignedIntLE(array1, 1));

        byte[] array2 = {(byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4, (byte) 0xf5, (byte) 0xf6};
        assertEquals(0xf4f3f2f1, ByteUtils.readUnsignedIntLE(array2, 0));
        assertEquals(0xf6f5f4f3, ByteUtils.readUnsignedIntLE(array2, 2));
    }

    @Test
    public void testReadUnsignedInt() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        long writeValue = 133444;
        ByteUtils.writeUnsignedInt(buffer, writeValue);
        buffer.flip();
        long readValue = ByteUtils.readUnsignedInt(buffer);
        assertEquals(writeValue, readValue);
    }

    @Test
    public void testWriteUnsignedIntLEToArray() {
        int value1 = 0x04030201;

        byte[] array1 = new byte[4];
        ByteUtils.writeUnsignedIntLE(array1, 0, value1);
        assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04}, array1);

        array1 = new byte[8];
        ByteUtils.writeUnsignedIntLE(array1, 2, value1);
        assertArrayEquals(new byte[] {0, 0, 0x01, 0x02, 0x03, 0x04, 0, 0}, array1);

        int value2 = 0xf4f3f2f1;

        byte[] array2 = new byte[4];
        ByteUtils.writeUnsignedIntLE(array2, 0, value2);
        assertArrayEquals(new byte[] {(byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4}, array2);

        array2 = new byte[8];
        ByteUtils.writeUnsignedIntLE(array2, 2, value2);
        assertArrayEquals(new byte[] {0, 0, (byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4, 0, 0}, array2);
    }

}
