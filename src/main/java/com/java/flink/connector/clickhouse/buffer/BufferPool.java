package com.java.flink.connector.clickhouse.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPool {
    static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);
    private final ArrayDeque<ByteBufferWithTs> free;
    private final ReentrantLock lock;
    private final long minCacheSize;
    private final long maxCacheSize;
    private final long keepAliveTimeMillis;
    private final long clearIntervalMs;
    private long lastClearTs;
    private long currentCacheSize;
    private long lastLogTs;

    public BufferPool(long minCacheSize, long maxCacheSize, long keepAliveTimeMillis) {
        this.free = new ArrayDeque<>();
        this.lock = new ReentrantLock();
        this.minCacheSize = minCacheSize;
        this.maxCacheSize = maxCacheSize;
        this.keepAliveTimeMillis = keepAliveTimeMillis;
        this.clearIntervalMs = Math.max(keepAliveTimeMillis / 10, 60000);
        this.lastClearTs = System.currentTimeMillis();
        this.currentCacheSize = 0;
        this.lastLogTs = System.currentTimeMillis();
    }

    ByteBuffer allocate(int size) {
        lock.lock();
        try {
            if (!free.isEmpty()) {
                ByteBuffer buffer = free.pollFirst().buffer;
                currentCacheSize -= buffer.capacity();
                return buffer;
            }
        } finally {
            lock.unlock();
        }
        return ByteBuffer.allocate(size);
    }

    void deallocate(ByteBuffer buffer) {
        lock.lock();
        try {
            if (currentCacheSize + buffer.capacity() <= maxCacheSize) {
                ((Buffer) buffer).clear();
                free.addFirst(new ByteBufferWithTs(buffer, System.currentTimeMillis()));
                currentCacheSize += buffer.capacity();
            } else {
                // 直接回收掉
            }
        } finally {
            lock.unlock();
        }

        clearExpiredBuffer();
    }

    public long getCurrentCacheSize() {
        return currentCacheSize;
    }

    private void clearExpiredBuffer() {
        long ts = System.currentTimeMillis();

        if(ts - lastLogTs > 300000){
            LOG.warn("currentCacheSize: " + (getCurrentCacheSize() >>> 20) + "M");
            lastLogTs = ts;
        }

        if (ts - lastClearTs < clearIntervalMs) {
            return;
        }
        lastClearTs = ts;

        lock.lock();
        try {
            ByteBufferWithTs ele = free.peekLast();
            while (ele != null && currentCacheSize - ele.buffer.capacity() >= minCacheSize && ts - ele.ts > keepAliveTimeMillis) {
                free.pollLast();
                currentCacheSize -= ele.buffer.capacity();
                ele = free.peekLast();
            }
        } finally {
            lock.unlock();
        }
    }

    static class ByteBufferWithTs {
        ByteBuffer buffer;
        long ts;

        public ByteBufferWithTs(ByteBuffer buffer, long ts) {
            this.buffer = buffer;
            this.ts = ts;
        }
    }
}
