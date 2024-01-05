package com.java.flink.stream.func;

import com.java.flink.util.SingleValueMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava31.com.google.common.io.Files;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.nio.MappedByteBuffer;

public class LocalLengthDelimitedFileUseMmapSourceFunctionV2 extends RichParallelSourceFunction<byte[]> {
    final private String filePath;
    final private long sleep;
    final private long numberOfRowsForSubtask;
    final private int cycleNum;
    private boolean stop;
    transient private MappedByteBuffer mmap;
    transient private  SingleValueMap.Data<int[]> contentPosData ;
    transient private int[] contentPos; // 每行内容结束pos

    public LocalLengthDelimitedFileUseMmapSourceFunctionV2(String filePath, long sleep, long numberOfRowsForSubtask, int cycleNum) {
        this.filePath = filePath;
        this.sleep = sleep;
        this.numberOfRowsForSubtask = numberOfRowsForSubtask;
        this.cycleNum = cycleNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        contentPosData = SingleValueMap.acquireData("mmap_content_pos_" + filePath, () -> getLinePos(filePath));
        contentPos = contentPosData.getData();
        mmap = Files.map(new File(filePath));
    }

    @Override
    public void run(SourceContext<byte[]> ctx) throws Exception {
        long rows = 0;
        int cycles = 0;
        int start;
        int end;
        int len;

        while (!stop && rows < numberOfRowsForSubtask && cycles < cycleNum) {
            for (int i = 0; i < contentPos.length; i++) {
                start = i == 0? 4: contentPos[i - 1] + 4;
                end = contentPos[i];
                len = end - start;
                mmap.position(start);

                byte[] bytes = new byte[len];
                mmap.get(bytes, 0, len);
                ctx.collect(bytes);

                rows += 1;
                if(rows >= numberOfRowsForSubtask){
                    break;
                }
                if(sleep > 0){
                    Thread.sleep(sleep);
                }
            }

            cycles += 1;
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }

    @Override
    public void close() throws Exception {
        if(contentPosData != null){
            contentPosData.release();
        }
    }

    private int[] getLinePos(String filePath) throws Exception{
        MappedByteBuffer byteBuffer = Files.map(new File(filePath));
        IntArrayList linePos = new IntArrayList(byteBuffer.limit() / (1024 * 16));
        int limit = byteBuffer.limit();
        int size;
        int read = 0;
        while (read < limit){
            size = byteBuffer.getInt(read);
            read += size + 4;
            linePos.add(read);
        }
        assert read == limit;

        return linePos.toIntArray();
    }
}
