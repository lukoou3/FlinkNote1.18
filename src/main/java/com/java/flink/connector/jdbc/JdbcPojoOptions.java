package com.java.flink.connector.jdbc;

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
    final Function<T, Object> keyExtractor;
    final boolean hasDelete;
    final List<String> deleteKeyFields;
    final Predicate<T> deleteDaTaPredicate;
    final BiFunction<T, T, T> replaceDaTaValue;

    private JdbcPojoOptions(Class<T> clazz, String tableName, boolean updateMode, List<String> oldValCols, Map<String, String> fieldColMap, PeriodExecSqlStrategy periodExecSqlStrategy, boolean keyedMode, Function<T, Object> keyExtractor, boolean hasDelete, List<String> deleteKeyFields, Predicate<T> deleteDaTaPredicate, BiFunction<T, T, T> replaceDaTaValue) {
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
        private Function<T, Object> keyExtractor = null;
        private boolean hasDelete = false;
        private List<String> deleteKeyFields = Collections.emptyList();
        private Predicate<T> deleteDaTaPredicate = null;
        private BiFunction<T, T, T> replaceDaTaValue = null;

        public Builder withClass(Class<T> clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withUpdateMode(boolean updateMode) {
            this.updateMode = updateMode;
            return this;
        }

        public Builder withOldValCols(List<String> oldValCols) {
            this.oldValCols = oldValCols;
            return this;
        }

        public Builder withFieldColMap(Map<String, String> fieldColMap) {
            this.fieldColMap = fieldColMap;
            return this;
        }

        public Builder withPeriodExecSqlStrategy(PeriodExecSqlStrategy periodExecSqlStrategy) {
            this.periodExecSqlStrategy = periodExecSqlStrategy;
            return this;
        }

        public Builder withKeyedMode(boolean keyedMode) {
            this.keyedMode = keyedMode;
            return this;
        }

        public Builder withKeyExtractor(Function<T, Object> keyExtractor) {
            this.keyExtractor = keyExtractor;
            return this;
        }

        public Builder withHasDelete(boolean hasDelete) {
            this.hasDelete = hasDelete;
            return this;
        }

        public Builder withDeleteKeyFields(List<String> deleteKeyFields) {
            this.deleteKeyFields = deleteKeyFields;
            return this;
        }

        public Builder withDeleteDaTaPredicate(Predicate<T> deleteDaTaPredicate) {
            this.deleteDaTaPredicate = deleteDaTaPredicate;
            return this;
        }

        public Builder withReplaceDaTaValue(BiFunction<T, T, T> replaceDaTaValue) {
            this.replaceDaTaValue = replaceDaTaValue;
            return this;
        }

        public JdbcPojoOptions build() {
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
            return new JdbcPojoOptions(clazz, tableName, updateMode, oldValCols, fieldColMap, periodExecSqlStrategy, keyedMode, keyExtractor, hasDelete, deleteKeyFields, deleteDaTaPredicate, replaceDaTaValue);
        }
    }
}
