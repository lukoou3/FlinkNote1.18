package com.java.flink.connector.jdbc;

import com.java.flink.util.function.SerializableBiFunction;
import com.java.flink.util.function.SerializableFunction;
import com.java.flink.util.function.SerializablePredicate;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class JdbcPojoOptions<T> implements Serializable {
    final Class<T> clazz;
    final String tableName;
    final boolean updateMode;
    final List<String> oldValCols;
    final Map<String, String> fieldColMap;
    final PeriodExecSqlStrategy periodExecSqlStrategy;
    final boolean keyedMode;
    final SerializableFunction<T, Object> keyExtractor;
    final boolean hasDelete;
    final List<String> deleteKeyFields;
    final SerializablePredicate<T> deleteDaTaPredicate;
    final SerializableBiFunction<T, T, T> replaceDaTaValue;

    private JdbcPojoOptions(Class<T> clazz, String tableName, boolean updateMode, List<String> oldValCols, Map<String, String> fieldColMap, PeriodExecSqlStrategy periodExecSqlStrategy, boolean keyedMode, SerializableFunction<T, Object> keyExtractor, boolean hasDelete, List<String> deleteKeyFields, SerializablePredicate<T> deleteDaTaPredicate, SerializableBiFunction<T, T, T> replaceDaTaValue) {
        this.clazz = clazz;
        this.tableName = tableName;
        this.updateMode = updateMode;
        this.oldValCols = oldValCols;
        this.fieldColMap = fieldColMap;
        this.periodExecSqlStrategy = periodExecSqlStrategy;
        this.keyedMode = keyedMode;
        this.keyExtractor = keyExtractor;
        this.hasDelete = hasDelete;
        this.deleteKeyFields = deleteKeyFields;
        this.deleteDaTaPredicate = deleteDaTaPredicate;
        this.replaceDaTaValue = replaceDaTaValue;
    }

    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }

    public static final class Builder<T> {
        private Class<T> clazz;
        private String tableName;
        private boolean updateMode = false;
        private List<String> oldValCols = Collections.emptyList();
        private Map<String, String> fieldColMap = Collections.emptyMap();
        private PeriodExecSqlStrategy periodExecSqlStrategy = null;
        private boolean keyedMode = false;
        // keyedMode参数
        private SerializableFunction<T, Object> keyExtractor = null;
        private boolean hasDelete = false;
        private List<String> deleteKeyFields = Collections.emptyList();
        private SerializablePredicate<T> deleteDaTaPredicate = null;
        private SerializableBiFunction<T, T, T> replaceDaTaValue = null;

        public Builder<T> withClass(Class<T> clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder<T> withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<T> withUpdateMode(boolean updateMode) {
            this.updateMode = updateMode;
            return this;
        }

        public Builder<T> withOldValCols(List<String> oldValCols) {
            this.oldValCols = oldValCols;
            return this;
        }

        public Builder<T> withFieldColMap(Map<String, String> fieldColMap) {
            this.fieldColMap = fieldColMap;
            return this;
        }

        public Builder<T> withPeriodExecSqlStrategy(PeriodExecSqlStrategy periodExecSqlStrategy) {
            this.periodExecSqlStrategy = periodExecSqlStrategy;
            return this;
        }

        public Builder<T> withKeyedMode(boolean keyedMode) {
            this.keyedMode = keyedMode;
            return this;
        }

        public Builder<T> withKeyExtractor(SerializableFunction<T, Object> keyExtractor) {
            this.keyExtractor = keyExtractor;
            return this;
        }

        public Builder<T> withHasDelete(boolean hasDelete) {
            this.hasDelete = hasDelete;
            return this;
        }

        public Builder<T> withDeleteKeyFields(List<String> deleteKeyFields) {
            this.deleteKeyFields = deleteKeyFields;
            return this;
        }

        public Builder<T> withDeleteDaTaPredicate(SerializablePredicate<T> deleteDaTaPredicate) {
            this.deleteDaTaPredicate = deleteDaTaPredicate;
            return this;
        }

        public Builder<T> withReplaceDaTaValue(SerializableBiFunction<T, T, T> replaceDaTaValue) {
            this.replaceDaTaValue = replaceDaTaValue;
            return this;
        }

        public JdbcPojoOptions<T> build() {
            Preconditions.checkNotNull(clazz);
            Preconditions.checkNotNull(tableName);
            if(keyedMode){
                Preconditions.checkNotNull(keyExtractor);
            }
            if(hasDelete){
                Preconditions.checkArgument(keyedMode);
                Preconditions.checkArgument(!deleteKeyFields.isEmpty());
                Preconditions.checkNotNull(deleteDaTaPredicate);
            }
            return new JdbcPojoOptions<T>(clazz, tableName, updateMode, oldValCols, fieldColMap, periodExecSqlStrategy, keyedMode, keyExtractor, hasDelete, deleteKeyFields, deleteDaTaPredicate, replaceDaTaValue);
        }
    }
}
