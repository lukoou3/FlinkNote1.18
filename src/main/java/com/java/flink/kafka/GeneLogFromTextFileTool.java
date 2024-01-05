package com.java.flink.kafka;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava31.com.google.common.io.Files;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Properties;

public class GeneLogFromTextFileTool {

    public static void main(String[] args) throws Exception{
        ParameterTool parameter = ParameterTool.fromArgs(args);
        final String filePath = parameter.getRequired("filePath");

        final String bootstrap = parameter.getRequired("bootstrap");
        final String topic = parameter.getRequired("topic");
        final int speed = parameter.getInt("speed", 20);
        final int parallelism = parameter.getInt("parallelism", 1);

        final int[] linePos = getLinePos(filePath);
        System.out.println("linePosLen:" + linePos.length);
        final MappedByteBuffer byteBuffer  = Files.map(new File(filePath));

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
                while (true){
                    ByteBuffer buffer = byteBuffer.duplicate();
                    byte[] bytes = new byte[1 << 18];
                    int start = 0;
                    int end = 0;
                    int len = 0;
                    for (int i = 0; i < linePos.length; i++) {
                        end = linePos[i];
                        len = end - start;
                        buffer.position(start);
                        start = end + 1;
                        if(len < 5){
                            continue;
                        }
                        buffer.get(bytes, 0, len);
                        //String line = new String(bytes, 0, len, StandardCharsets.UTF_8);
                        //JSON.parseObject(line);
                        byte[] b = new byte[len];
                        System.arraycopy(bytes, 0, b, 0, len);
                        //JSON.parseObject(new String(b, StandardCharsets.UTF_8));
                        //System.out.println(new String(b, StandardCharsets.UTF_8));
                        producer.send(new ProducerRecord<>(topic,  b));

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
                            //System.out.println(line);
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

    public static int[] getLinePos(String filePath) throws Exception{
        MappedByteBuffer byteBuffer = Files.map(new File(filePath));
        IntArrayList linePos = new IntArrayList(512);
        int limit = byteBuffer.limit();
        for (int i = 0; i < limit; i++) {
            if(byteBuffer.get(i) == '\n'){
                linePos.add(i);
            }
        }
        return linePos.toIntArray();
    }
}
