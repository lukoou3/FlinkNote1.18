package com.java.flink.connector.clickhouse.table;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;

import java.util.Properties;

public class ClickHouseDynamicSink implements DynamicTableSink {
    private final RowType rowType;
    private final int batchSize;
    private final int batchByteSize;
    private final long batchIntervalMs;
    private final String host;
    private final String table;
    private final Properties connInfo;

    public ClickHouseDynamicSink(RowType rowType, int batchSize, int batchByteSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        this.rowType = rowType;
        this.batchSize = batchSize;
        this.batchByteSize = batchByteSize;
        this.batchIntervalMs = batchIntervalMs;
        this.host = host;
        this.table = table;
        this.connInfo = connInfo;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowDataBatchIntervalClickHouseSink func = new RowDataBatchIntervalClickHouseSink(rowType, batchSize, batchByteSize, batchIntervalMs, host, table, connInfo);
        return SinkFunctionProvider.of(func);
    }

    @Override
    public DynamicTableSink copy() {
        return new ClickHouseDynamicSink(rowType, batchSize, batchByteSize, batchIntervalMs, host, table, connInfo);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouseTableSink";
    }
}
