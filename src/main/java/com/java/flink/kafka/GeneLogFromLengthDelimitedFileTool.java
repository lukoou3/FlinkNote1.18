package com.java.flink.kafka;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava31.com.google.common.io.Files;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Properties;

public class GeneLogFromLengthDelimitedFileTool {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        final String filePath = parameter.getRequired("filePath");

        final String bootstrap = parameter.getRequired("bootstrap");
        final String topic = parameter.getRequired("topic");
        final int speed = parameter.getInt("speed", 20);
        final int parallelism = parameter.getInt("parallelism", 1);

        int[] contentPos = getContentPos(filePath);
        System.out.println("contentPosLen:" + contentPos.length);
        final MappedByteBuffer byteBuffer = Files.map(new File(filePath));

        Thread[] threads = new Thread[parallelism];
        for (int j = 0; j < threads.length; j++) {
            int finalI = j + 1;
            Thread thread = new Thread(() -> {
                Properties props = new Properties();
                props.put("bootstrap.servers", bootstrap);
                props.put("acks", "1");
                props.put("retries", 0);
                props.put("linger.ms", 50);
                props.put("compression.type", "snappy");
                props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

                int count = 0;
                long startMs = System.currentTimeMillis();
                int start;
                int end;
                int len;
                while (true){
                    ByteBuffer buffer = byteBuffer.duplicate();
                    for (int i = 0; i < contentPos.length; i++) {
                        start = i == 0? 4: contentPos[i - 1] + 4;
                        end = contentPos[i];
                        len = end - start;
                        buffer.position(start);

                        byte[] bytes = new byte[len];
                        buffer.get(bytes, 0, len);

                        producer.send(new ProducerRecord<>(topic,  bytes));

                        count++;
                        if(count == speed){
                            count = 0;
                            long t = System.currentTimeMillis() - startMs;
                            if(t < 1000){
                                try {
                                    Thread.sleep(1000 - t);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            t = System.currentTimeMillis() - startMs;
                            startMs = System.currentTimeMillis();
                            System.out.println(finalI +  "-" + new java.sql.Timestamp(startMs) + ":" + speed + "," + t);
                        }
                    }
                }


            }, "Thread:" +(j + 1) + "/" + parallelism);
            threads[j] = thread;
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

    }

    private static int[] getContentPos(String filePath) throws Exception{
        MappedByteBuffer byteBuffer = Files.map(new File(filePath));
        IntArrayList contentPos = new IntArrayList(byteBuffer.limit() / (1024 * 16));
        int limit = byteBuffer.limit();
        int size;
        int read = 0;
        while (read < limit){
            size = byteBuffer.getInt(read);
            read += size + 4;
            contentPos.add(read);
        }

        Preconditions.checkArgument(read == limit);

        return contentPos.toIntArray();
    }
}
