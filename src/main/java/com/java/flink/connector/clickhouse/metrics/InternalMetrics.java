package com.java.flink.connector.clickhouse.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class InternalMetrics {
    private final MetricGroup metricGroup;
    private final Counter errorEvents;
    private final Counter droppedEvents;
    private final Counter inEvents;
    private final Counter outEvents;
    private final Counter inBytes;
    private final Counter outBytes;

    public InternalMetrics(RuntimeContext runtimeContext) {
        metricGroup = runtimeContext.getMetricGroup().addGroup("internal_metrics");
        errorEvents = metricGroup.counter("error_events");
        droppedEvents = metricGroup.counter("dropped_events");
        inEvents = metricGroup.counter("in_events");
        outEvents = metricGroup.counter("out_events");
        inBytes = metricGroup.counter("in_bytes");
        outBytes = metricGroup.counter("out_bytes");
    }

    public void incrementErrorEvents() {
        errorEvents.inc();
    }
    public void incrementDroppedEvents() {
        droppedEvents.inc();
    }
    public void incrementInEvents() {
        inEvents.inc();
    }
    public void incrementOutEvents() {
        outEvents.inc();
    }
    public void incrementErrorEvents(long num) {
        errorEvents.inc(num);
    }
    public void incrementDroppedEvents(long num) {
        droppedEvents.inc(num);
    }
    public void incrementInEvents(long num) {
        inEvents.inc(num);
    }
    public void incrementOutEvents(long num) {
        outEvents.inc(num);
    }
    public void incrementInBytes(long bytes) {
        inBytes.inc(bytes);
    }

    public void incrementOutBytes(long bytes) {
        outBytes.inc(bytes);
    }
}