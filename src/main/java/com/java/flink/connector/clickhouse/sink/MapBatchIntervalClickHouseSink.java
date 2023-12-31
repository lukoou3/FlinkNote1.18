package com.java.flink.connector.clickhouse.sink;

import com.alibaba.fastjson2.JSON;

import com.github.housepower.data.Block;

import java.util.Map;
import java.util.Properties;

public class MapBatchIntervalClickHouseSink extends AbstractBatchIntervalClickHouseSink<Map<String, Object>>{

    public MapBatchIntervalClickHouseSink(int batchSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        super(batchSize, batchIntervalMs, host, table, connInfo);
    }

    @Override
    boolean addBatch(Block batch, Map<String, Object> map) throws Exception {
        Object value;
        for (int i = 0; i < columnNames.length; i++) {
            value = map.get(columnNames[i]);

            if (value == null) {
                value = columnDefaultValues[i];
                batch.setObject(i, value); // 默认值不用转换
            } else {
                // int columnIdx = batch.paramIdx2ColumnIdx(i);
                // batch.setObject(columnIdx, convertToCkDataType(columnTypes[i], value));
                // batch.setObject(i, convertToCkDataType(dataType, value));
                try {
                    batch.setObject(i, columnConverters[i].convert(value));
                } catch (Exception e) {
                    throw new RuntimeException(columnNames[i] + "列转换值出错:" + value + ", event data:" + JSON.toJSONString(map), e);
                }
            }
        }

        return true;
    }

}
