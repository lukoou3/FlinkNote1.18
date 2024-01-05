package com.java.flink.types;

public class ArrayType extends DataType {
    public DataType elementType;

    public ArrayType(DataType elementType) {
        this.elementType = elementType;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        ArrayType arrayType = (ArrayType) o;
        return elementType.equals(arrayType.elementType);
    }

    @Override
    public String simpleString() {
        return String.format("array<%s>", elementType.simpleString());
    }

    void buildFormattedString(String prefix, StringBuilder sb, int maxDepth){
        if (maxDepth > 0) {
            sb.append(String.format("%s-- element: %s\n", prefix, elementType.typeName()));
            Types.buildFormattedString(elementType, prefix + "    |", sb, maxDepth);
        }
    }
}
