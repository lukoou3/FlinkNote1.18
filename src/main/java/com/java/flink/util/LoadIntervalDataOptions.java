package com.java.flink.util;

import java.io.Serializable;

public class LoadIntervalDataOptions implements Serializable {
    private static final int DEFAULT_INTERVAL_MILLIS = 1000 * 60 * 10;
    public static final int DEFAULT_MAX_RETRY_TIMES = 3;
    public static final int DEFAULT_RETRY_WAIT_MILLIS = 1000;
    public static final boolean DEFAULT_FAIL_ON_EXCEPTION = true;

    private final long intervalMs;
    private final int maxRetries;
    private final long retryWaitMs;
    private final boolean failOnException;

    protected LoadIntervalDataOptions(long intervalMs, int maxRetries, long retryWaitMs, boolean failOnException) {
        this.intervalMs = intervalMs;
        this.maxRetries = maxRetries;
        this.retryWaitMs = retryWaitMs;
        this.failOnException = failOnException;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryWaitMs() {
        return retryWaitMs;
    }

    public boolean isFailOnException() {
        return failOnException;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static LoadIntervalDataOptions defaults() {
        return builder().build();
    }

    public static final class Builder {
        private long intervalMs = DEFAULT_INTERVAL_MILLIS;
        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;
        private long retryWaitMs = DEFAULT_RETRY_WAIT_MILLIS;
        private boolean failOnException = DEFAULT_FAIL_ON_EXCEPTION;

        public Builder withIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public LoadIntervalDataOptions build() {
            return new LoadIntervalDataOptions(intervalMs, maxRetries, retryWaitMs, failOnException);
        }
    }

}
