package com.java.flink.connector.jdbc;

import java.io.Serializable;

public class JdbcExecutionOptions implements Serializable {
    public static final int DEFAULT_SIZE = 1000;
    private static final int DEFAULT_INTERVAL_MILLIS = 5000;
    private static final int DEFAULT_MIN_PAUSE_BETWEEN_FLUSH_MILLIS = 100;
    public static final int DEFAULT_MAX_RETRY_TIMES = 2;

    private final int batchSize;
    private final long batchIntervalMs;
    private final long minPauseBetweenFlushMs;
    private final int maxRetries;

    private JdbcExecutionOptions(int batchSize, long batchIntervalMs, long minPauseBetweenFlushMs, int maxRetries) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.minPauseBetweenFlushMs = minPauseBetweenFlushMs;
        this.maxRetries = maxRetries;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public long getMinPauseBetweenFlushMs() {
        return minPauseBetweenFlushMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JdbcExecutionOptions defaults() {
        return builder().build();
    }

    public static final class Builder {
        private int batchSize = DEFAULT_SIZE;
        private long batchIntervalMs = DEFAULT_INTERVAL_MILLIS;
        private long minPauseBetweenFlushMs = DEFAULT_MIN_PAUSE_BETWEEN_FLUSH_MILLIS;
        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withBatchIntervalMs(long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public Builder withMaxRetries(long minPauseBetweenFlushMs) {
            this.minPauseBetweenFlushMs = minPauseBetweenFlushMs;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public JdbcExecutionOptions build() {
            return new JdbcExecutionOptions(batchSize, batchIntervalMs, minPauseBetweenFlushMs, maxRetries);
        }
    }
}
