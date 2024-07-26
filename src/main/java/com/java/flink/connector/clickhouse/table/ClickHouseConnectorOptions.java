package com.java.flink.connector.clickhouse.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

import static com.java.flink.connector.clickhouse.table.ClickHouseConnectorOptionsUtil.CONNECTION_INFO_PREFIX;

public class ClickHouseConnectorOptions {
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("clickhouse table name.");

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("clickhouse host and tcp port info. format: host1:port,host2:port");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch.size")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("The flush max size , over this number of records, will flush data.");

    public static final ConfigOption<MemorySize> BATCH_BYTE_SIZE =
            ConfigOptions.key("batch.byte.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("200mb"))
                    .withDescription("The flush max buffer byte size , over this byte size, will flush data.");

    public static final ConfigOption<Duration> BATCH_INTERVAL =
            ConfigOptions.key("batch.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("The flush interval mills, over this time, asynchronous threads will flush data.");

    public static final ConfigOption<String> CONNECTION_USER =
            ConfigOptions.key(CONNECTION_INFO_PREFIX + "user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required clickhouse connection user.");
    public static final ConfigOption<String> CONNECTION_PASSWORD =
            ConfigOptions.key(CONNECTION_INFO_PREFIX + "password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required clickhouse connection password.");
}
