package com.java.flink.util;

import com.java.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoadIntervalDataUtil<T> {
    static final Logger LOG = LoggerFactory.getLogger(LoadIntervalDataUtil.class);
    private LoadIntervalDataOptions options;
    private SupplierWithException<T> dataSupplier;
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean stopped = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;
    private Exception exception;
    private T data;

    private LoadIntervalDataUtil(SupplierWithException<T> dataSupplier, LoadIntervalDataOptions options) {
        this.options = options;
        this.dataSupplier = dataSupplier;
    }

    public static <T> LoadIntervalDataUtil<T> newInstance(SupplierWithException<T> dataSupplier, LoadIntervalDataOptions options){
        LoadIntervalDataUtil<T> loadIntervalDataUtil = new LoadIntervalDataUtil(dataSupplier, options);
        loadIntervalDataUtil.start();
        return loadIntervalDataUtil;
    }

    public T data() throws Exception {
        if(!options.isFailOnException() || exception == null){
            return data;
        } else{
            throw exception;
        }
    }

    private void updateData(){
        try {
            LOG.warn("updateData start....");
            T newData = Utils.retry(dataSupplier, options.getMaxRetries(), options.getRetryWaitMs(), "LoadIntervalDataUtil-updateData");
            data = newData;
            LOG.warn("updateData end....");
        }catch (Exception e) {
            exception = e;
            LOG.error("updateDataError", e);
        }
    }

    private void start(){
        if (started.compareAndSet(false, true)){
            updateData();
            this.scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("LoadIntervalDataUtil");
            this.scheduler.scheduleWithFixedDelay(() -> updateData(), options.getIntervalMs(),  options.getIntervalMs(), TimeUnit.MILLISECONDS);
            LOG.warn("start....");
        }
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            if(scheduler != null){
                this.scheduler.shutdown();
            }
            LOG.warn("stop....");
        }
    }
}
