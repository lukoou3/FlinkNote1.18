package com.java.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Set;

public class JsonFormatFactory implements DeserializationFormatFactory {
    public static final ConfigOption<Boolean> OBJECT_REUSE = ConfigOptions.key("object-reuse") .booleanType().defaultValue(true);

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        FactoryUtil.validateFactoryOptions(this, readableConfig);
        final boolean objectReuse = readableConfig.get(OBJECT_REUSE);
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
                RowType rowType = (RowType)producedDataType.getLogicalType();
                TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(producedDataType);
                return new JsonRowDataSimpleDeserializationSchema(rowType, rowDataTypeInfo, objectReuse);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return "jackson";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(OBJECT_REUSE);
        return options;
    }
}
