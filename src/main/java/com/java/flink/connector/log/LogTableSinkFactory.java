package com.java.flink.connector.log;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.java.flink.connector.log.LogConnectorOptions.INSERT_ONLY;
import static com.java.flink.connector.log.LogConnectorOptions.LOG_MODE;

public class LogTableSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "log";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();

        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate();

        DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        LogMode logMode = Optional.ofNullable(LogMode.fromName(config.get(LOG_MODE))).orElse(LogMode.LOG_INFO);

        return new LogTableSink(
                logMode,
                encodingFormat,
                physicalDataType,
                config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null),
                config.get(INSERT_ONLY)
        );
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.SINK_PARALLELISM);
        options.add(LOG_MODE);
        options.add(INSERT_ONLY);
        return options;
    }


}
