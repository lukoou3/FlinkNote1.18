package com.java.flink.types;

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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return simpleString();
    }
}
