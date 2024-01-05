package com.java.flink.types;

public class BinaryType extends DataType {

    BinaryType() {
    }

    @Override
    public String simpleString() {
        return "binary";
    }
}
