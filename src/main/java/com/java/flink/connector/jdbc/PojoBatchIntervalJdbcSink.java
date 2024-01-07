package com.java.flink.connector.jdbc;

import com.java.flink.util.JavaReflection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PojoBatchIntervalJdbcSink<T> extends BatchIntervalJdbcSink<T> {
    private final Class<T> clazz;
    private final String[] fieldNames;
    private final String updateSql;
    private transient Method[] readMethods;

    private final String[] delFieldNames;
    private final String delSql;
    private transient Method[] delFieldReadMethods;
    private final Function<T, Object> keyExtractor;
    private final Predicate<T> deleteDaTaPredicate;
    private final BiFunction<T, T, T> replaceDaTaValue;


    public PojoBatchIntervalJdbcSink(JdbcConnectionOptions connectionOptions, JdbcExecutionOptions executionOptions, JdbcPojoOptions<T> pojoOptions) {
        super(connectionOptions, executionOptions,
                pojoOptions.keyedMode, pojoOptions.periodExecSqlStrategy, pojoOptions.hasDelete);
        this.clazz = pojoOptions.clazz;
        this.fieldNames = getFieldNames();
        String[] cols = Arrays.stream(this.fieldNames).map(name -> pojoOptions.fieldColMap.getOrDefault(name, name)).toArray(String[]::new);
        this.updateSql = geneJdbcPreparedUpdateSql(pojoOptions.tableName, cols, pojoOptions.oldValCols, pojoOptions.updateMode);
        if(pojoOptions.hasDelete){
            this.delFieldNames = Arrays.stream(this.fieldNames).filter(x -> pojoOptions.deleteKeyFields.contains(x)).toArray(String[]::new);
            Preconditions.checkArgument(delFieldNames.length == pojoOptions.deleteKeyFields.size(), "deleteKeyFields存在未知的属性");
            String[] delCols = Arrays.stream(this.delFieldNames).map(name -> pojoOptions.fieldColMap.getOrDefault(name, name)).toArray(String[]::new);
            this.delSql = geneJdbcPreparedDelSql(pojoOptions.tableName, delCols);
        }else{
            this.delFieldNames = null;
            this.delSql = null;
        }
        this.keyExtractor = pojoOptions.keyExtractor;
        this.deleteDaTaPredicate = pojoOptions.deleteDaTaPredicate;
        this.replaceDaTaValue = pojoOptions.replaceDaTaValue;
    }

    @Override
    protected void onInit(Configuration parameters) throws Exception {
        super.onInit(parameters);
        Map<String, PropertyDescriptor> propertyDescriptorMap = Arrays.stream(JavaReflection.getJavaBeanReadableAndWritableProperties(clazz)).collect(Collectors.toMap(f -> f.getName(), f -> f));
        this.readMethods = getFieldReadMethods(propertyDescriptorMap, fieldNames);
        this.delFieldReadMethods = getFieldReadMethods(propertyDescriptorMap, delFieldNames);
    }

    private Method[] getFieldReadMethods(Map<String, PropertyDescriptor> propertyDescriptorMap, String[] fieldNames) {
        Method[] readMethods = new Method[fieldNames.length];
        for (int i = 0; i < readMethods.length; i++) {
            PropertyDescriptor propertyDescriptor = propertyDescriptorMap.get(fieldNames[i]);
            if(propertyDescriptor == null){
                throw new IllegalArgumentException(fieldNames[i]);
            }
            readMethods[i] = propertyDescriptor.getReadMethod();
        }
        return readMethods;
    }

    @Override
    protected String getUpdateSql() {
        return updateSql;
    }

    @Override
    protected void setStmt(PreparedStatement stmt, T data) throws Exception {
        Object value;
        for (int i = 0; i < readMethods.length; i++) {
            value = readMethods[i].invoke(data);
            stmt.setObject(i+1, value);
        }
    }

    @Override
    protected Object getKey(T data) {
        return keyExtractor.apply(data);
    }

    @Override
    protected boolean isDeleteData(T data) {
        return deleteDaTaPredicate.test(data);
    }

    @Override
    protected T replaceValue(T newValue, T oldValue) {
        if(replaceDaTaValue == null){
            return super.replaceValue(newValue, oldValue);
        }else{
            return replaceDaTaValue.apply(newValue, oldValue);
        }
    }

    @Override
    protected String getDeleteSql() {
        return delSql;
    }
    @Override
    protected void setDeleteStmt(PreparedStatement stmt, T data) throws Exception {
        Object value;
        for (int i = 0; i < delFieldReadMethods.length; i++) {
            value = delFieldReadMethods[i].invoke(data);
            stmt.setObject(i+1, value);
        }
    }

    private String[] getFieldNames(){
        PropertyDescriptor[] propertyDescriptors;
        try {
            propertyDescriptors = JavaReflection.getJavaBeanReadableAndWritableProperties(clazz);
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }
        List<String> nameList = new ArrayList<>(propertyDescriptors.length);
        PropertyDescriptor property;
        for (int i = 0; i < propertyDescriptors.length; i++) {
            property = propertyDescriptors[i];
            Class<?> returnType = property.getReadMethod().getReturnType();
            if(returnType == Integer.class || returnType == int.class){

            } else if (returnType == Long.class || returnType == long.class) {

            } else if (returnType == Float.class || returnType == float.class) {

            } else if (returnType == Double.class || returnType == double.class) {

            } else if (returnType == String.class) {

            } else if (returnType == java.sql.Timestamp.class) {

            }else {
                throw  new UnsupportedOperationException("unsupported type:" + returnType);
            }
            nameList.add(property.getName());
        }
        return nameList.toArray(new String[nameList.size()]);
    }

    private String geneJdbcPreparedUpdateSql(String tableName, String[] cols, List<String> oldValCols, boolean updateMode){
        String columns = String.join(",", cols);
        String placeholders = String.join(",", Arrays.stream(cols).map(col -> "?").collect(Collectors.toList()));
        if (updateMode) {
            String duplicateSetting = String.join(",", Arrays.stream(cols).map(col -> {
                if(oldValCols.contains(col)){
                    return String.format("%s=IFNULL(%s,VALUES(%s))", col, col, col);
                }else{
                    return String.format("%s=VALUES(%s)", col, col);
                }
            }).collect(Collectors.toList()));
            return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", tableName, columns, placeholders, duplicateSetting);
        } else {
            return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
        }
    }

    private String geneJdbcPreparedDelSql(String tableName, String[] delCols){
        String delWhere = String.join(" and ", Arrays.stream(delCols).map(col -> col + " = ?").collect(Collectors.toList()));
        String delSql = String.format("delete from %s where %s", tableName, delWhere);
        return delSql;
    }
}
