package com.java.flink.connector.clickhouse.buffer;

import com.github.housepower.buffer.BuffedWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class ReusedByteArrayWriter implements BuffedWriter {
    private final int blockSize;
    private final BufferPool bufferPool;
    private ByteBuffer buffer;

    private final List<ByteBuffer> byteBufferList = new LinkedList<>();

    public ReusedByteArrayWriter(int blockSize, BufferPool bufferPool) {
        this.blockSize = blockSize;
        this.bufferPool = bufferPool;
        reuseOrAllocateByteBuffer();
    }

    @Override
    public void writeBinary(byte byt) throws IOException {
        buffer.put(byt);
        flushToTarget(false);
    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {
        writeBinary(bytes, 0, bytes.length);
    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {

        while (buffer.remaining() < length) {
            int num = buffer.remaining();
            buffer.put(bytes, offset, num);
            flushToTarget(true);

            offset += num;
            length -= num;
        }

        buffer.put(bytes, offset, length);
        flushToTarget(false);
    }

    @Override
    public void flushToTarget(boolean force) throws IOException {
        if (buffer.hasRemaining() && !force) {
            return;
        }
        reuseOrAllocateByteBuffer();
    }

    public List<ByteBuffer> getBufferList() {
        return byteBufferList;
    }

    public void reset() {
        byteBufferList.forEach(b -> {
            // upcast is necessary, see detail at:
            // https://bitbucket.org/ijabz/jaudiotagger/issues/313/java-8-javalangnosuchmethoderror
            // ((Buffer) b).clear();
            bufferPool.deallocate(b);
        });
        byteBufferList.clear();

        reuseOrAllocateByteBuffer();
    }

    private ByteBuffer reuseOrAllocateByteBuffer() {
        ByteBuffer newBuffer = bufferPool.allocate(blockSize);
        if (newBuffer == null) {
            newBuffer = ByteBuffer.allocate(blockSize);
        }

        buffer = newBuffer;
        byteBufferList.add(buffer);
        return buffer;
    }
}
