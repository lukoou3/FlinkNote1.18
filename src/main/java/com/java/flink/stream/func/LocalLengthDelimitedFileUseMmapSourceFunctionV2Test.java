package com.java.flink.stream.func;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.protobuf.Descriptors;
import com.java.flink.formats.protobuf.ProtobufSerializer;
import com.java.flink.formats.protobuf.ProtobufUtils;
import com.java.flink.formats.protobuf.SchemaConverters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class LocalLengthDelimitedFileUseMmapSourceFunctionV2Test {
    static final Logger LOG = LoggerFactory.getLogger(LocalLengthDelimitedFileUseMmapSourceFunctionV2Test.class);

    @Test
    public void testWriteJson() throws Exception {
        String filePath = "files/online_log_length_delimited_json.txt";
        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        raf.setLength(Integer.MAX_VALUE);
        FileChannel rafChannel = raf.getChannel();
        MappedByteBuffer mmap = rafChannel.map(FileChannel.MapMode.READ_WRITE, 0, raf.length());

        int length = 0;
        try(FileInputStream inputStream = new FileInputStream("files/online_log.json")){
            LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
            while (lines.hasNext()){
                String line = lines.next().trim();
                if(line.isEmpty()){
                    continue;
                }
                byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                mmap.putInt(bytes.length);
                mmap.put(bytes);
                length += bytes.length + 4;
            }
        }

        mmap.force();
        /* Windows or z/OS won't let us modify the file length while the file is mmapped :-( */
        ByteBufferUnmapper.unmap(filePath, mmap);
        raf.setLength(length);
        rafChannel.close();
        raf.close();
    }

    @Test
    public void testReadJson() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filePath = "files/online_log_length_delimited_json.txt";
        DataStreamSource<byte[]> ds = env.addSource(new LocalLengthDelimitedFileUseMmapSourceFunctionV2(filePath, 1000, Long.MAX_VALUE, 1));
        DataStream<String> lines = ds.map(x -> new String(x, StandardCharsets.UTF_8));

        lines.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                JSON.parseObject(value);
                LOG.warn(value);
            }
        });

        env.execute("testReadJson");
    }

    // protoc -o online_log.desc online_log.proto
    @Test
    public void testWriteProto() throws Exception {
        String path = "files/online_log.desc";
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"OnlineLog");
        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);

        String filePath = "files/online_log_length_delimited_proto.txt";
        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        try(FileInputStream inputStream = new FileInputStream("files/online_log.json")){
            LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
            while (lines.hasNext()){
                String line = lines.next().trim();
                if(line.isEmpty()){
                    continue;
                }
                JSONObject map = JSON.parseObject(line);
                byte[] bytes = serializer.serialize(map);
                raf.writeInt(bytes.length);
                raf.write(bytes);
            }
        }
        raf.close();
    }

    @Test
    public void testReadProto() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = "files/online_log.desc";
        byte[] content = ProtobufUtils.readDescriptorFileContent(path);


        String filePath = "files/online_log_length_delimited_proto.txt";
        DataStreamSource<byte[]> ds = env.addSource(new LocalLengthDelimitedFileUseMmapSourceFunctionV2(filePath, 10, Long.MAX_VALUE, 1));

        ds.addSink(new RichSinkFunction<byte[]>() {
            int count = 0;
            SchemaConverters.MessageConverter messageConverter;

            @Override
            public void open(Configuration parameters) throws Exception {
                Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(content,"OnlineLog");
                messageConverter = new SchemaConverters.MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), false);
            }

            @Override
            public void invoke(byte[] bytes, Context context) throws Exception {
                count++;
                Map<String, Object> map = messageConverter.converter(bytes);
                //LOG.warn(JSON.toJSONString(map));
                System.out.println(count + ":" + JSON.toJSONString(map));
            }
        });

        env.execute("testReadJson");
    }
}
