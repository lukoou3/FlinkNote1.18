package com.java.flink.stream.func;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.FileInputStream;

public class LocalTextFileSourceFunction extends RichParallelSourceFunction<String> {
    private String filePath;
    private long sleep;
    private long numberOfRowsForSubtask;
    private int cycleNum;
    private boolean stop;

    public LocalTextFileSourceFunction(String filePath, long sleep, long numberOfRowsForSubtask, int cycleNum) {
        this.filePath = filePath;
        this.sleep = sleep;
        this.numberOfRowsForSubtask = numberOfRowsForSubtask;
        this.cycleNum = cycleNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long rows = 0;
        int cycles = 0;

        while (!stop && rows < numberOfRowsForSubtask && cycles < cycleNum) {
            try(FileInputStream inputStream = new FileInputStream(filePath)){
                LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
                while (!stop && lines.hasNext() && rows < numberOfRowsForSubtask) {
                    String line = lines.next().trim();
                    if(!line.isEmpty()){
                        ctx.collect(line);
                        rows += 1;
                        if(sleep > 0){
                            Thread.sleep(sleep);
                        }
                    }
                }

                cycles += 1;
            }
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }

}
