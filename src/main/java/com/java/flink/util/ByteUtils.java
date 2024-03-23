package com.java.flink.util;

import java.nio.ByteBuffer;

// org.apache.kafka.common.utils.ByteUtils
public final class ByteUtils {
    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final ByteBuffer EMPTY_BUF = ByteBuffer.wrap(EMPTY_BYTES);
    public static final int MAX_VARINT_SIZE = 5;
    public static final int MAX_VARINT64_SIZE = 10;

    /**
     * 写入varint, 参考Google Protocol Buffers
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    /**
     * zig-zag编码后写入varint, 参考Google Protocol Buffers
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeSignedVarint(int value, ByteBuffer buffer) {
        writeUnsignedVarint((value << 1) ^ (value >> 31), buffer);
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readUnsignedVarint(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return value;
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readSignedVarint(ByteBuffer buffer) {
        int value = readUnsignedVarint(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * 写入varlong, 参考Google Protocol Buffers
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeUnsignedVarlong(long value, ByteBuffer buffer) {
        while ((value & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    /**
     * zig-zag编码后写入varlong, 参考Google Protocol Buffers
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeSignedVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        writeUnsignedVarlong(v, buffer);
    }

    /**
     * Read a long stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readUnsignedVarlong(ByteBuffer buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return value;
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readSignedVarlong(ByteBuffer buffer)  {
        long value = readUnsignedVarlong(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Number of bytes needed to encode an integer in unsigned variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfUnsignedVarint(int value) {
        int bytes = 1;
        while ((value & 0xffffff80) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfSignedVarint(int value) {
        return sizeOfUnsignedVarint((value << 1) ^ (value >> 31));
    }

    /**
     * Number of bytes needed to encode a long in unsigned variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfUnsignedVarlong(long value) {
        int bytes = 1;
        while ((value & 0xffffffffffffff80L) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }

    public static int sizeOfSignedVarlong(long value) {
        return sizeOfUnsignedVarlong((value << 1) ^ (value >> 63));
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeDouble(double value, ByteBuffer buffer) {
        buffer.putDouble(value);
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     */
    public static double readDouble(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3]   = (byte) (value >>> 24);
    }

    /**
     * 写uint32
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * 写uint32
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset] << 0 & 0xff)
                | ((buffer[offset + 1] & 0xff) << 8)
                | ((buffer[offset + 2] & 0xff) << 16)
                | ((buffer[offset + 3] & 0xff) << 24);
    }

    /**
     * 读取uint32, buffer position增加4
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * 从position读取uint32, 不改变buffer position
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
