package com.sketch.bitset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * 实现bitset
 * 参考: https://github.com/apache/spark/blob/master/common/sketch/src/main/java/org/apache/spark/util/sketch/BitArray.java, 这个在spark中是为了实现BloomFilterImpl
 */
final class BitArray {
    private final long[] data;
    private long bitCount;

    static int numWords(long numBits) {
        if (numBits <= 0) {
            throw new IllegalArgumentException("numBits must be positive, but got " + numBits);
        }
        long numWords = (long) Math.ceil(numBits / 64.0);
        if (numWords > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Can't allocate enough space for " + numBits + " bits");
        }
        return (int) numWords;
    }

    public BitArray(long numBits) {
        this(new long[numWords(numBits)]);
    }

    private BitArray(long[] data) {
        this.data = data;
        long bitCount = 0;
        for (long word : data) {
            bitCount += Long.bitCount(word);
        }
        this.bitCount = bitCount;
    }

    /** Returns true if the bit changed value. */
    public boolean set(long index) {
        if (!get(index)) {
            data[(int) (index >>> 6)] |= (1L << index);
            bitCount++;
            return true;
        }
        return false;
    }

    public boolean get(long index) {
        return (data[(int) (index >>> 6)] & (1L << index)) != 0;
    }

    /** Number of bits */
    public long bitSize() {
        return (long) data.length * Long.SIZE;
    }

    /** Number of set bits (1s) */
    public long cardinality() {
        return bitCount;
    }

    /** Combines the two BitArrays using bitwise OR. */
    public void putAll(BitArray array) {
        assert data.length == array.data.length : "BitArrays must be of equal length when merging";
        long bitCount = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] |= array.data[i];
            bitCount += Long.bitCount(data[i]);
        }
        this.bitCount = bitCount;
    }

    /** Combines the two BitArrays using bitwise AND. */
    public void and(BitArray array) {
        assert data.length == array.data.length : "BitArrays must be of equal length when merging";
        long bitCount = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] &= array.data[i];
            bitCount += Long.bitCount(data[i]);
        }
        this.bitCount = bitCount;
    }

    void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(data.length);
        for (long datum : data) {
            out.writeLong(datum);
        }
    }

    static BitArray readFrom(DataInputStream in) throws IOException {
        int numWords = in.readInt();
        long[] data = new long[numWords];
        for (int i = 0; i < numWords; i++) {
            data[i] = in.readLong();
        }
        return new BitArray(data);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof BitArray)){
            return false;
        }
        return Arrays.equals(data, ((BitArray)other).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
