package com.java.flink.stream.func;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava31.com.google.common.io.Files;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

@Deprecated
public class LocalTextFileUseMmapSourceFunction extends RichParallelSourceFunction<String> {
    private String filePath;
    private long sleep;
    private long numberOfRowsForSubtask;
    private int cycleNum;
    private boolean stop;
    transient private MappedByteBuffer mmap;
    transient private int[] linePos;

    public LocalTextFileUseMmapSourceFunction(String filePath, long sleep, long numberOfRowsForSubtask, int cycleNum) {
        this.filePath = filePath;
        this.sleep = sleep;
        this.numberOfRowsForSubtask = numberOfRowsForSubtask;
        this.cycleNum = cycleNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        linePos = getLinePos(filePath);
        mmap = Files.map(new File(filePath));
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long rows = 0;
        int cycles = 0;
        byte[] bytes = new byte[1024 * 32];

        while (!stop && rows < numberOfRowsForSubtask && cycles < cycleNum) {
            int start = 0;
            int end = 0;
            int len = 0;
            for (int i = 0; i < linePos.length; i++) {
                end = linePos[i];
                len = end - start;
                mmap.position(start);
                start = end + 1;
                if(len < 5){
                    continue;
                }
                if(len > bytes.length){
                    bytes = new byte[len];
                }

                mmap.get(bytes, 0, len);
                String str = new String(bytes, 0, len, StandardCharsets.UTF_8);
                ctx.collect(str);

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

    private int[] getLinePos(String filePath) throws Exception{
        MappedByteBuffer byteBuffer = Files.map(new File(filePath));
        IntArrayList linePos = new IntArrayList(byteBuffer.limit() / (1024 * 32));
        int limit = byteBuffer.limit();
        for (int i = 0; i < limit; i++) {
            if(byteBuffer.get(i) == '\n'){
                linePos.add(i);
            }
        }
        return linePos.toIntArray();
    }
}
