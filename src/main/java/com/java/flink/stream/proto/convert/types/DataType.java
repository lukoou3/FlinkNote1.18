package com.java.flink.stream.proto.convert.types;

import java.io.Serializable;

public abstract class DataType implements Serializable {
    public abstract String simpleString();

    public String typeName(){
        String typeName = this.getClass().getSimpleName();
        if(typeName.endsWith("Type")){
            typeName = typeName.substring(0, typeName.length() - 4);
        }
        return typeName.toLowerCase();
    }

    @Override
    public String toString() {
        return simpleString();
    }
}
