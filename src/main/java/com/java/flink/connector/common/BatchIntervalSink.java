package com.java.flink.connector.common;

import com.java.flink.util.ThreadUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BatchIntervalSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    static final Logger LOG = LoggerFactory.getLogger(BatchIntervalSink.class);
    final private int batchSize;
    final private long batchIntervalMs;
    final private long minPauseBetweenFlushMs;
    final private boolean keyedMode;
    transient private boolean closed;
    transient private ScheduledExecutorService scheduler;
    transient private ScheduledFuture<?> scheduledFuture;
    transient private ReentrantLock lock;
    transient private List<T> batch;
    transient private LinkedHashMap<Object, T> keyedBatch;
    transient private Exception flushException;
    transient private long lastFlushTs = 0L;
    transient private long writeCount = 0L;
    transient private long writeBytes = 0L;
    transient private Counter numRecordsOutCounter;
    transient private Counter numBytesOutCounter;

    public BatchIntervalSink(int batchSize, long batchIntervalMs, long minPauseBetweenFlushMs, boolean keyedMode) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.minPauseBetweenFlushMs = minPauseBetweenFlushMs;
        this.keyedMode = keyedMode;
    }

    protected abstract void onInit(Configuration parameters) throws Exception;

    protected abstract void onFlush(Collection<T> datas) throws Exception;

    protected abstract void onClose() throws Exception;

    protected T valueTransform(T data){
        return data;
    }

    protected Object getKey(T data){
        throw new RuntimeException("keyedMode必须实现");
    }

    protected T replaceValue(T newValue, T oldValue){
        return newValue;
    }

    @Override
    public final void open(Configuration parameters) throws Exception {
        onInit(parameters);
        numBytesOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumBytesOutCounter();
        numRecordsOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        lock = new ReentrantLock();
        if(!keyedMode){
            batch = new ArrayList<>();
        }else{
            keyedBatch = new LinkedHashMap<>();
        }
        lastFlushTs = 0L;
        if (batchIntervalMs != 0 && batchSize != 1) {
            final String threadName = "BatchIntervalSink-" + (getRuntimeContext().getIndexOfThisSubtask() + 1) + "/" + getRuntimeContext().getNumberOfParallelSubtasks();
            //scheduler = Executors.newScheduledThreadPool(1, new org.apache.flink.util.concurrent.ExecutorThreadFactory(threadName));
            this.scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor(threadName);
            scheduledFuture = scheduler.scheduleWithFixedDelay(() -> {
                if(System.currentTimeMillis() - lastFlushTs < minPauseBetweenFlushMs) {
                    return;
                }

                lock.lock();
                try {
                    flush();
                } catch (Throwable t) {
                    if(t instanceof Exception){
                        flushException = (Exception) t;
                    }else{
                        flushException = new Exception(t);
                    }
                } finally {
                    lock.unlock();
                }

            }, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public final void checkFlushException(){
        if (flushException != null)
            throw new RuntimeException("flush failed.", flushException);
    }

    public final int currentBatchCount(){
        return !keyedMode? batch.size(): keyedBatch.size();
    }

    @Override
    public final void invoke(T value, Context context) throws Exception {
        checkFlushException();
        lock.lock();
        try {
            if(!keyedMode){
                batch.add(valueTransform(value));
            }else{
                T newValue = valueTransform(value);
                Object key = getKey(newValue);
                T oldValue = keyedBatch.get(key);
                if(oldValue == null){
                    keyedBatch.put(key, newValue);
                }else{
                    keyedBatch.put(key, replaceValue(newValue, oldValue));
                }
            }

            if (batchSize > 0 && currentBatchCount() >= batchSize) {
                flush();
            }
        }finally {
            lock.unlock();
        }
    }

    final void incNumBytesOut(long bytes){
        writeBytes += bytes;
        numBytesOutCounter.inc(bytes);
    }

    public final void flush() throws Exception{
        checkFlushException();
        lastFlushTs = System.currentTimeMillis();
        if(currentBatchCount() <= 0){
            return;
        }
        lock.lock();
        try {
            long start = System.currentTimeMillis();
            int currentBatchCount = this.currentBatchCount();
            LOG.warn("flush " + currentBatchCount +" start:" + new java.sql.Timestamp(start));

            if(!keyedMode){
                onFlush(batch);
                writeCount += currentBatchCount;
                numRecordsOutCounter.inc(currentBatchCount);
                batch.clear();
            }else{
                onFlush(keyedBatch.values());
                writeCount += currentBatchCount;
                numRecordsOutCounter.inc(currentBatchCount);
                keyedBatch.clear();
            }

            LOG.warn("flush " + currentBatchCount + " end:" + new java.sql.Timestamp(System.currentTimeMillis()) + "," + (System.currentTimeMillis() - start));

        }finally {
            lock.unlock();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public final void close() throws Exception {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                this.scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            // init中可能抛出异常
            if(lock != null){
                lock.lock();
                try {
                    if (currentBatchCount() > 0) {
                        flush();
                    }
                } catch (Throwable t) { // // 这里必须的, 防止走不到onClose
                    if(t instanceof Exception){
                        flushException = (Exception) t;
                    }else{
                        flushException = new Exception(t);
                    }
                } finally {
                    lock.unlock();
                }
            }

            onClose();
        }

        checkFlushException();
    }
}
