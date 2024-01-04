package com.java.flink.connector.localfile;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class LocalFileSource<OUT> implements Source<OUT, LocalFileSource.LocalFileSplit, LocalFileSource.LocalFileCheckpoint>, ResultTypeQueryable<OUT> {
    final private String filePath;
    final private long sleep;
    final private long numberOfRowsForSubtask;
    final private int cycleNum;
    final private DeserializationSchema<OUT> deserializer;

    public LocalFileSource(String filePath, long sleep, long numberOfRowsForSubtask, int cycleNum, DeserializationSchema<OUT> deserializer) {
        this.filePath = filePath;
        this.sleep = sleep;
        this.numberOfRowsForSubtask = numberOfRowsForSubtask;
        this.cycleNum = cycleNum;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<LocalFileSplit, LocalFileCheckpoint> createEnumerator(SplitEnumeratorContext<LocalFileSplit> enumContext) throws Exception {
        int parallelism = enumContext.currentParallelism();
        List<LocalFileSplit> splits = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            splits.add(new LocalFileSplit(String.valueOf(i)));
        }
        return null;
    }

    @Override
    public SplitEnumerator<LocalFileSplit, LocalFileCheckpoint> restoreEnumerator(SplitEnumeratorContext<LocalFileSplit> enumContext, LocalFileCheckpoint checkpoint) throws Exception {
        // 不支持从checkpoint恢复
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LocalFileSplit> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LocalFileCheckpoint> getEnumeratorCheckpointSerializer() {
        // 不支持从checkpoint恢复
        return null;
    }

    @Override
    public SourceReader<OUT, LocalFileSplit> createReader(SourceReaderContext readerContext) throws Exception {
        deserializer.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });
        return new SocketReader();
    }

    private class SocketReader implements SourceReader<OUT, LocalFileSplit> {
        private MyIterator<OUT> iterator;
        private boolean stop;
        private long rows;
        private int cycles;
        private FileInputStream inputStream;
        private LineIterator lines;
        private OUT data;

        @Override
        public void start() {
            if(iterator != null){
                return;
            }
            try {
                inputStream = new FileInputStream(filePath);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            lines = IOUtils.lineIterator(inputStream, "utf-8");
            iterator = new MyIterator<OUT>() {
                @Override
                public boolean hasNext() throws Exception{
                    if(data == null){
                        while (!stop && rows < numberOfRowsForSubtask && cycles < cycleNum){
                            while (lines.hasNext()){
                                String line = lines.next().trim();
                                if(!line.isEmpty()){
                                    byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                                    data = deserializer.deserialize(bytes);
                                    rows += 1;
                                    return true;
                                }
                            }
                            cycles += 1;
                            inputStream.close();
                            inputStream = new FileInputStream(filePath);
                            lines = IOUtils.lineIterator(inputStream, "utf-8");
                        }
                    }
                    return data != null;
                }

                @Override
                public OUT next() throws Exception{
                    return data;
                }
            };
        }

        @Override
        public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
            if (iterator.hasNext()){
                output.collect(data);
                if(sleep > 0){
                    Thread.sleep(sleep);
                }
                return InputStatus.MORE_AVAILABLE;
            }
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public List<LocalFileSplit> snapshotState(long checkpointId) {
            // This source is not fault-tolerant.
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            // Not used. The socket is read in a blocking manner until it is closed.
            return null;
        }

        @Override
        public void addSplits(List<LocalFileSplit> splits) {
            // Ignored. The socket itself implicitly represents the only split.
        }

        @Override
        public void notifyNoMoreSplits() {
            // Ignored. The socket itself implicitly represents the only split.
        }

        @Override
        public void close() throws Exception {
            stop = true;
            if(inputStream != null){
                inputStream.close();
            }
        }
    }

    public static class LocalFileSplit  implements SourceSplit {
        private final String splitId;

        public LocalFileSplit(String splitId) {
            this.splitId = splitId;
        }

        @Override
        public String splitId() {
            return splitId;
        }
    }

    private static final class SplitSerializer implements SimpleVersionedSerializer<LocalFileSource.LocalFileSplit> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(LocalFileSplit split) throws IOException {
            final DataOutputSerializer out = new DataOutputSerializer(split.splitId().length() + 2);
            out.writeUTF(split.splitId());
            return out.getCopyOfBuffer();
        }

        @Override
        public LocalFileSplit deserialize(int version, byte[] serialized) throws IOException {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return new LocalFileSplit(in.readUTF());
        }
    }

    /**
     * Placeholder because the SocketSource does not support fault-tolerance and thus does not require actual checkpointing.
     */
    public static class LocalFileCheckpoint {}

    public interface MyIterator<E> {
        boolean hasNext() throws Exception;

        E next() throws Exception;
    }
}
