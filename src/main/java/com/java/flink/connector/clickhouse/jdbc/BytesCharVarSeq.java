package com.java.flink.connector.clickhouse.jdbc;

public class BytesCharVarSeq implements CharSequence {

    private byte[] bytes;
    private int len;

    public BytesCharVarSeq(byte[] bytes, int len) {
        this.bytes = bytes;
        this.len = len;
    }

    public void setBytesAndLen(byte[] bytes, int len) {
        this.bytes = bytes;
        this.len = len;
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) bytes[index];
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        byte[] newBytes = new byte[end - start];
        System.arraycopy(bytes, start, newBytes, 0, end - start);
        return new BytesCharVarSeq(newBytes, end - start);
    }

    @Override
    public String toString() {
        return "BytesCharVarSeq, length: " + length();
    }

    public byte[] bytes() {
        return bytes;
    }
}
