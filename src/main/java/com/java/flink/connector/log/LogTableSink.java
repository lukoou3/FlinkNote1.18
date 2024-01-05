package com.java.flink.connector.log;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

/**
 * 定义并行度。默认情况下，并行度由框架决定，和链在一起的上游 operator 一致。
 * 认为并行度是2，上游是mysocket，并行度是1，LogTableSink默认并行度是1，可以配置为2
 */
public class LogTableSink implements DynamicTableSink {
    private final LogMode logMode;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType producedDataType;
    private final @Nullable Integer parallelism;
    private final boolean insertOnly; // 用于校验表结果的模式

    public LogTableSink(LogMode logMode, EncodingFormat<SerializationSchema<RowData>> encodingFormat, DataType producedDataType, @Nullable Integer parallelism, boolean insertOnly) {
        this.logMode = logMode;
        this.encodingFormat = encodingFormat;
        this.producedDataType = producedDataType;
        this.parallelism = parallelism;
        this.insertOnly = insertOnly;
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if(insertOnly){
            return ChangelogMode.insertOnly();
        }else{
            // 全部返回
            // requestedMode
            // 不想看到before
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .build();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(context, producedDataType);
        LogSinkFunction<RowData> func = new LogSinkFunction<>(logMode, serializer);
        return SinkFunctionProvider.of(func, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new LogTableSink(logMode, encodingFormat, producedDataType, parallelism, insertOnly);
    }

    @Override
    public String asSummaryString() {
        return "log-sink";
    }
}
