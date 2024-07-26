package com.java.flink.connector.clickhouse.util;

public class BlockColumnsByteSizeInfo {
    public long totalSize;
    public long totalBufferSize;
    public String bigColumnsInfo;

    public BlockColumnsByteSizeInfo(long totalSize, long totalBufferSize, String bigColumnsInfo) {
        this.totalSize = totalSize;
        this.totalBufferSize = totalBufferSize;
        this.bigColumnsInfo = bigColumnsInfo;
    }
}
