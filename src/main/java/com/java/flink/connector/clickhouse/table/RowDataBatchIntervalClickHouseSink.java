package com.java.flink.connector.clickhouse.table;

import com.alibaba.fastjson2.JSON;
import com.github.housepower.data.Block;
import com.github.housepower.data.IDataType;
import com.github.housepower.data.type.*;
import com.github.housepower.data.type.complex.*;
import com.github.housepower.exception.ClickHouseSQLException;
import com.java.flink.connector.clickhouse.jdbc.DataTypeStringV2;
import com.java.flink.connector.clickhouse.sink.AbstractBatchIntervalClickHouseSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.*;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

public class RowDataBatchIntervalClickHouseSink extends AbstractBatchIntervalClickHouseSink<RowData> {
    private final RowType rowType;
    private RowValueConverter[] rowValueConverters;

    public RowDataBatchIntervalClickHouseSink(RowType rowType, int batchSize, int batchByteSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        super(batchSize, batchByteSize, batchIntervalMs, host, table, rowType.getFields().stream().map(x -> x.getName()).toArray(String[]::new), connInfo);
        this.rowType = rowType;
    }

    @Override
    protected void onInit(Configuration parameters) throws Exception {
        super.onInit(parameters);
        List<RowType.RowField> fields = rowType.getFields();
        rowValueConverters = new RowValueConverter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i).getName();
            LogicalType logicalType = fields.get(i).getType();
            Preconditions.checkArgument(name.equals(columnNames[i]));
            rowValueConverters[i] = makeRowConverter(logicalType, columnTypes[i]);
        }
    }

    @Override
    protected int addBatch(Block batch, RowData row) throws Exception {
        int writeSize = 0;
        Object value;
        for (int i = 0; i < rowValueConverters.length; i++) {
            if (row.isNullAt(i)) {
                value = columnDefaultValues[i];
                batch.setObject(i, value); // 默认值不用转换
                writeSize += columnDefaultSizes[i];
            } else {
                try {
                    writeSizeHelper.size = 0;
                    batch.setObject(i, columnConverters[i].convert(rowValueConverters[i].convert(row, i), writeSizeHelper));
                    writeSize += writeSizeHelper.size;
                } catch (Exception e) {
                    throw new RuntimeException(columnNames[i] + "列转换值出错:" + row, e);
                }
            }
        }

        return writeSize;
    }

    private RowValueConverter makeRowConverter(LogicalType logicalType, IDataType<?, ?> type) {
        if (type instanceof DataTypeNullable) {
            return makeRowConverter(logicalType, ((DataTypeNullable) type).getNestedDataType());
        }

        if (type instanceof DataTypeDate || type instanceof DataTypeDate32) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (row, i) -> row.getString(i).toString();
                case DATE:
                    return (row, i) -> LocalDate.ofEpochDay(row.getInt(i));
                default:
                    throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
            }
        }

        if (type instanceof DataTypeDateTime || type instanceof DataTypeDateTime64) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (row, i) -> row.getString(i).toString();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    final int precision = getPrecision(logicalType);
                    return (row, i) -> row.getTimestamp(i, precision).getMillisecond();
                case BIGINT:
                    return (row, i) -> row.getLong(i);
                default:
                    throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
            }
        }

        if (type instanceof DataTypeArray) {
            switch (logicalType.getTypeRoot()) {
                case ARRAY:
                    ArrayValueConverter eleConverter = makeArrayConverter(((ArrayType) logicalType).getElementType(), ((DataTypeArray) type).getElemDataType());
                    return (row, i) -> {
                        ArrayData array = row.getArray(i);
                        List<Object> list = new ArrayList<>(array.size());
                        for (int j = 0; j < array.size(); j++) {
                            if (array.isNullAt(j)) {
                                list.add(null);
                            } else {
                                list.add(eleConverter.convert(array, j));
                            }
                        }
                        return list;
                    };
                default:
                    throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
            }
        }

        if (type instanceof DataTypeString || type instanceof DataTypeFixedString || type instanceof DataTypeStringV2) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (row, i) -> row.getString(i).toBytes();
                case BINARY:
                case VARBINARY:
                    return (row, i) -> row.getBinary(i);
                default:
                    break;
            }
        }

        if (type instanceof DataTypeInt8 || type instanceof DataTypeUInt8 || type instanceof DataTypeInt16 || type instanceof DataTypeUInt16
                || type instanceof DataTypeInt32 || type instanceof DataTypeUInt32 || type instanceof DataTypeInt64 || type instanceof DataTypeUInt64
                || type instanceof DataTypeFloat32 || type instanceof DataTypeFloat64 || type instanceof DataTypeDecimal || type instanceof DataTypeUUID
                || type instanceof DataTypeNothing || type instanceof DataTypeString || type instanceof DataTypeFixedString || type instanceof DataTypeStringV2) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (row, i) -> row.getString(i).toString();
                case INTEGER:
                    return (row, i) -> row.getInt(i);
                case BIGINT:
                    return (row, i) -> row.getLong(i);
                case FLOAT:
                    return (row, i) -> row.getFloat(i);
                case DOUBLE:
                    return (row, i) -> row.getDouble(i);
                default:
                    throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
            }
        }

        throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
    }

    private ArrayValueConverter makeArrayConverter(LogicalType logicalType, IDataType<?, ?> type) {
        if (type instanceof DataTypeNullable) {
            return makeArrayConverter(logicalType, ((DataTypeNullable) type).getNestedDataType());
        }

        if (type instanceof DataTypeDate || type instanceof DataTypeDate32) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (array, i) -> array.getString(i).toString();
                case DATE:
                    return (array, i) -> LocalDate.ofEpochDay(array.getInt(i));
                default:
                    throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
            }
        }

        if (type instanceof DataTypeDateTime || type instanceof DataTypeDateTime64) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (array, i) -> array.getString(i).toString();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    final int precision = getPrecision(logicalType);
                    return (array, i) -> array.getTimestamp(i, precision).getMillisecond();
                case BIGINT:
                    return (array, i) -> array.getLong(i);
                default:
                    throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
            }
        }

        if (type instanceof DataTypeString || type instanceof DataTypeFixedString || type instanceof DataTypeStringV2) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (array, i) -> array.getString(i).toBytes();
                case BINARY:
                case VARBINARY:
                    return (array, i) -> array.getBinary(i);
                default:
                    break;
            }
        }

        if (type instanceof DataTypeInt8 || type instanceof DataTypeUInt8 || type instanceof DataTypeInt16 || type instanceof DataTypeUInt16
                || type instanceof DataTypeInt32 || type instanceof DataTypeUInt32 || type instanceof DataTypeInt64 || type instanceof DataTypeUInt64
                || type instanceof DataTypeFloat32 || type instanceof DataTypeFloat64 || type instanceof DataTypeDecimal || type instanceof DataTypeUUID
                || type instanceof DataTypeNothing || type instanceof DataTypeString || type instanceof DataTypeFixedString || type instanceof DataTypeStringV2) {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return (array, i) -> array.getString(i).toString();
                case INTEGER:
                    return (array, i) -> array.getInt(i);
                case BIGINT:
                    return (array, i) -> array.getLong(i);
                case FLOAT:
                    return (array, i) -> array.getFloat(i);
                case DOUBLE:
                    return (array, i) -> array.getDouble(i);
                default:
                    throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
            }
        }

        throw new UnsupportedOperationException(String.format("unsupported type converter: %s => %s", logicalType, type));
    }

    @FunctionalInterface
    public interface RowValueConverter extends Serializable {
        Object convert(RowData row, int i) throws ClickHouseSQLException;
    }

    @FunctionalInterface
    public interface ArrayValueConverter extends Serializable {
        Object convert(ArrayData array, int i) throws ClickHouseSQLException;
    }
}
