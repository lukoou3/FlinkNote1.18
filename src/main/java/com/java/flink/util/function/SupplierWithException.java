package com.java.flink.util.function;

@FunctionalInterface
public interface SupplierWithException<T> {
    T get() throws Exception;
}
