package com.java.flink.stream.proto.convert.types;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StructType extends DataType {
    public final StructField[] fields;

    public StructType(StructField[] fields) {
        this.fields = fields;
        validateFields(fields);
    }

    @Override
    public String simpleString() {
        return String.format("struct<%s>", Arrays.stream(fields).map(f -> f.name + ":" + f.dataType.simpleString()).collect(Collectors.joining(", ")));
    }

    public String treeString() {
        return treeString(Integer.MAX_VALUE);
    }

    public String treeString(int maxDepth) {
        StringBuilder sb = new StringBuilder();
        sb.append("root\n");
        String prefix = " |";
        int depth = maxDepth > 0? maxDepth: Integer.MAX_VALUE;
        for (StructField field : fields) {
            field.buildFormattedString(prefix, sb, depth);
        }
        return sb.toString();
    }

    void buildFormattedString(String prefix, StringBuilder sb, int maxDepth){
        for (StructField field : fields) {
            field.buildFormattedString(prefix, sb, maxDepth);
        }
    }

    private static void validateFields(StructField[] fields) {
        List<String> fieldNames = Arrays.stream(fields).map(f -> f.name).collect(Collectors.toList());
        if (fieldNames.stream().anyMatch(StringUtils::isBlank)) {
            throw new IllegalArgumentException("Field names must contain at least one non-whitespace character.");
        }

        final Set<String> duplicates =
                fieldNames.stream()
                        .filter(n -> Collections.frequency(fieldNames, n) > 1)
                        .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Field names must be unique. Found duplicates: %s", duplicates));
        }
    }

    public static final class StructField implements Serializable {
        public final String name;
        public final DataType dataType;

        public StructField(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        void buildFormattedString(String prefix, StringBuilder sb, int maxDepth){
            if(maxDepth > 0){
                sb.append(String.format("%s-- %s: %s\n", prefix, name, dataType.typeName()));
                Types.buildFormattedString(dataType, prefix + "    |", sb, maxDepth);
            }
        }
    }

}
