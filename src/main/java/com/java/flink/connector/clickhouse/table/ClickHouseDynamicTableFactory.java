package com.java.flink.connector.clickhouse.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.java.flink.connector.clickhouse.table.ClickHouseConnectorOptions.*;
import static com.java.flink.connector.clickhouse.table.ClickHouseConnectorOptionsUtil.*;

public class ClickHouseDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "clickhouse";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validateExcept(CONNECTION_INFO_PREFIX);

        RowType rowType = (RowType) context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType().getLogicalType();

        String host = config.get(HOST);
        String table = config.get(TABLE);
        int batchSize = config.get(BATCH_SIZE);
        MemorySize batchByteMemorySize = config.get(BATCH_BYTE_SIZE);
        Preconditions.checkArgument(batchByteMemorySize.getMebiBytes() < 1000, "batch.byte.size can not bigger than 1000m");
        int batchByteSize = (int) batchByteMemorySize.getBytes();
        long batchIntervalMs = config.get(BATCH_INTERVAL).toMillis();
        Properties connInfo = ClickHouseConnectorOptionsUtil.getClickHouseConnInfo(context.getCatalogTable().getOptions());

        return new ClickHouseDynamicSink(rowType, batchSize, batchByteSize, batchIntervalMs, host, table, connInfo);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLE);
        options.add(HOST);
        options.add(CONNECTION_USER);
        options.add(CONNECTION_PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BATCH_SIZE);
        options.add(BATCH_BYTE_SIZE);
        options.add(BATCH_INTERVAL);
        return options;
    }
}
