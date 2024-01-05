package com.java.flink.connector.log;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LogConnectorOptions {
    public static final ConfigOption<String> LOG_MODE = ConfigOptions.key("log-mode").stringType().defaultValue(LogMode.LOG_INFO.name().toLowerCase());
    public static final ConfigOption<Boolean> INSERT_ONLY = ConfigOptions.key("insert-only").booleanType().defaultValue(false);

}
