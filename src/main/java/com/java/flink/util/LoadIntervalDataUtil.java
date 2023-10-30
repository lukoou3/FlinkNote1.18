package com.java.flink.util;

import com.java.flink.util.function.SupplierWithException;

public class LoadIntervalDataUtil<T> {

    private LoadIntervalDataOptions options;
    private SupplierWithException<T> dataSupplier;

    private LoadIntervalDataUtil(LoadIntervalDataOptions options, SupplierWithException<T> dataSupplier) {
        this.options = options;
        this.dataSupplier = dataSupplier;
    }

    private void updateData(){

    }
}
