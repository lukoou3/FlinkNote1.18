package com.java.flink.stream.proto.convert.types;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.java.flink.stream.proto.convert.types.StructType.StructField;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Types {
    public static final IntegerType INT = new IntegerType();
    public static final LongType BIGINT = new LongType();
    public static final StringType STRING = new StringType();
    public static final FloatType FLOAT = new FloatType();
    public static final DoubleType DOUBLE = new DoubleType();
    public static final BooleanType BOOLEAN = new BooleanType();
    public static final BinaryType BINARY = new BinaryType();

    public static final Pattern ARRAY_RE = Pattern.compile("array\\s*<(.+)>", Pattern.CASE_INSENSITIVE);
    public static final Pattern STRUCT_RE = Pattern.compile("struct\\s*<(.+)>", Pattern.CASE_INSENSITIVE);

    public static StructType parseSchemaFromJson(String jsonFields) {
        JSONArray fieldArray = JSON.parseArray(jsonFields);
        StructField[] fields = new StructField[fieldArray.size()];

        for (int i = 0; i < fieldArray.size(); i++) {
            JSONObject fieldObject = fieldArray.getJSONObject(i);
            String name = fieldObject.getString("name").trim();
            String type = fieldObject.getString("type").trim();
            DataType dataType = parseDataType(type);
            fields[i] = new StructField(name, dataType);
        }

        return new StructType(fields);
    }

    static DataType parseDataType(String type){
        type = type.trim();
        if("int".equalsIgnoreCase(type)){
           return INT;
        } else if ("bigint".equalsIgnoreCase(type)){
            return BIGINT;
        } else if ("string".equalsIgnoreCase(type)){
            return STRING;
        } else if ("float".equalsIgnoreCase(type)){
            return FLOAT;
        } else if ("double".equalsIgnoreCase(type)){
            return DOUBLE;
        }

        // array类型
        Matcher matcher = ARRAY_RE.matcher(type);
        if(matcher.matches()){
            String eleType = matcher.group(1);
            DataType elementType = parseDataType(eleType);
            return new ArrayType(elementType);
        }

        // struct类型
        matcher = STRUCT_RE.matcher(type);
        if(matcher.matches()){
            List<StructField> fields = new ArrayList<>();
            String str = matcher.group(1);
            int startPos = 0, endPos = -1;
            int i = startPos + 1;
            int level = 0;
            while (i < str.length()){
                while (i < str.length()){
                    if(str.charAt(i) == ':'){
                        endPos = i;
                        break;
                    }
                    i++;
                }

                if(endPos <= startPos){
                    throw new UnsupportedOperationException("不支持的类型:" + type);
                }

                String name = str.substring(startPos, endPos).trim();
                startPos = i + 1;
                endPos = -1;
                i = startPos + 1;
                while (i < str.length()){
                    if(str.charAt(i) == ',' && level == 0){
                        endPos = i;
                        break;
                    }
                    if(str.charAt(i) == '<'){
                        level++;
                    }
                    if(str.charAt(i) == '>'){
                        level--;
                    }
                    i++;
                }

                if(i == str.length()){
                    endPos = i;
                }
                if(endPos <= startPos){
                    throw new UnsupportedOperationException("不支持的类型:" + type);
                }

                String tp = str.substring(startPos, endPos).trim();
                fields.add(new StructField(name, parseDataType(tp)));

                i++;
                startPos = i;
                endPos = -1;
            }

            return new StructType(fields.toArray(new StructField[fields.size()]));
        }

        throw new UnsupportedOperationException("不支持的类型:" + type);
    }

    static void buildFormattedString(DataType dataType, String prefix, StringBuilder sb, int maxDepth){
        if(dataType instanceof ArrayType){
            ((ArrayType)dataType).buildFormattedString(prefix, sb, maxDepth - 1);
        } else if (dataType instanceof StructType) {
            ((StructType)dataType).buildFormattedString(prefix, sb, maxDepth - 1);
        }
    }
}
