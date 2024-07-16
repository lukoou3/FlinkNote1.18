package com.java.flink.connector.clickhouse.table;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Properties;

public class ClickHouseConnectorOptionsUtil {
    public static final String CONNECTION_INFO_PREFIX = "connection.";

    public static Properties getClickHouseConnInfo(Map<String, String> tableOptions) {
        final Properties connInfo = new Properties();

        if (hasClickHouseConnInfo(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(CONNECTION_INFO_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((CONNECTION_INFO_PREFIX).length());
                                if(!StringUtils.isBlank(value)){
                                    connInfo.put(subKey, value);
                                }
                            });
        }
        return connInfo;
    }

    /**
     * Decides if the table options contains Kafka client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasClickHouseConnInfo(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(CONNECTION_INFO_PREFIX));
    }
}
