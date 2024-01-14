package com.java.flink.stream.theory;

import com.java.flink.stream.func.LogMap;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

public class TypeInformationTest {

    /**
     * Types中预定义了STRING、INT、LONG等TypeInformation，使用这些TypeInformation时直接这些static final值就行
     */
    @Test
    public void preDefineTypeTest() throws Exception {
        TypeInformation<String> strTypeInformation1 = TypeInformation.of(String.class);
        TypeInformation<String> strTypeInformation2 = Types.STRING;
        System.out.println(strTypeInformation1.getClass());
        System.out.println(strTypeInformation2.getClass());
        // TypeInformation.of(String.class) 和 Types.STRING 是equals相等的
        System.out.println(strTypeInformation1.equals(strTypeInformation2));

        String str = "莫南";
        TypeSerializer<String> serializer = strTypeInformation1.createSerializer(null);
        assert strTypeInformation1.getClass() == BasicTypeInfo.class;
        assert serializer.copy(str) == str;
        assert serializer.getClass() == StringSerializer.class;
        assert serializer == StringSerializer.INSTANCE;
    }

    /**
     * PojoData的TypeInformation是PojoTypeInfo，对应的TypeSerializer是PojoSerializer
     * PojoSerializer<PojoData> 的copy和serialize、deserialize可以直接点进去看
     * 要清楚copy和serialize、deserialize方法分别在什么时候调用：
     *    不启用enableObjectReuse时单个Chain之前的算子之间使用CopyingChainingOutput，调用copy复制元素
     *    多个task之间调用serialize、deserialize实现网络传输
     */
    @Test
    public void pojoTypeTest() throws Exception {
        TypeInformation<PojoData> pojoType = TypeInformation.of(PojoData.class);
        TypeSerializer<PojoData> serializer = pojoType.createSerializer(new ExecutionConfig());
        assert PojoTypeInfo.class == pojoType.getClass();
        assert PojoSerializer.class == serializer.getClass();

        PojoData data = new PojoData(1, "莫南");
        PojoData data1 = serializer.copy(data);
        assert data != data1;


        DataOutputSerializer dataOutput = new DataOutputSerializer(1024);
        serializer.serialize(data, dataOutput);
        System.out.println("position:" + dataOutput.length());
        ByteBuffer readBuffer = dataOutput.wrapAsByteBuffer();
        System.out.println("limit:" + readBuffer.limit());
        DataInputDeserializer dataInput = new DataInputDeserializer(readBuffer.duplicate());
        System.out.println("position:" + dataInput.getPosition());
        PojoData data2 = serializer.deserialize(dataInput);
        System.out.println("position:" + dataInput.getPosition());
        assert data != data2;

        System.out.println(data);
        System.out.println(data1);
        System.out.println(data2);

        /**
         * PojoSerializer<PojoData> 的copy和serialize、deserialize可以直接点进去看
         * 就是一个个属性写入的，先写入field是否为null，后写入field的值
         */
        data.setId(2);
        data.setName("燕青丝");
        dataOutput.setPosition(0);
        ((PojoSerializer<PojoData>) serializer).serialize(data, dataOutput);
        readBuffer = dataOutput.wrapAsByteBuffer();

        dataInput = new DataInputDeserializer(readBuffer.duplicate());
        data2 = ((PojoSerializer<PojoData>) serializer).deserialize(dataInput);
        System.out.println(data2);
    }

    /**
     * org.apache.flink.api.java.typeutils.TypeExtractor#analyzePojo // 判断是否是pojo
     *     org.apache.flink.api.java.typeutils.TypeExtractor#getAllDeclaredFields // 获取全部属性
     *     org.apache.flink.api.java.typeutils.TypeExtractor#isValidPojoField // 判断属性是否是pojo属性，public或者有get set方法
     *
     */
    @Test
    public void isPojoTypeTest() throws Exception {
        TypeInformation<PojoData> typeInformation = TypeInformation.of(PojoData.class);
        System.out.println(typeInformation.getClass());
        System.out.println(typeInformation.createSerializer(new ExecutionConfig()).getClass());
        System.out.println();

        // 属性全部定义public也被推断为PojoTypeInfo
        TypeInformation<ImplicitPojoData> typeInformation1 = TypeInformation.of(ImplicitPojoData.class);
        System.out.println(typeInformation1.getClass());
        System.out.println(typeInformation1.createSerializer(new ExecutionConfig()).getClass());
        System.out.println();

        TypeInformation<NoPojoData> typeInformation2 = TypeInformation.of(NoPojoData.class);
        System.out.println(typeInformation2.getClass());
        System.out.println(typeInformation2.createSerializer(new ExecutionConfig()).getClass());
        System.out.println();

        // 虽然NoPojoData2是PojoTypeInfo，但它的datas属性是GenericType
        TypeInformation<NoPojoData2> typeInformation3 = TypeInformation.of(NoPojoData2.class);
        System.out.println(typeInformation3.getClass());
        System.out.println(typeInformation3.createSerializer(new ExecutionConfig()).getClass());
        System.out.println();
    }

    @Test
    public void rowTest() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));

        System.out.println(dataStream.getType());
        System.out.println(TypeExtractor.getForObject(Row.of("Alice", 12)));
        System.out.println(TypeExtractor.getForObject(Row.of("Alice", 12.2)));


        dataStream.map(new LogMap<>())
                .print();

        env.execute();
    }

    @Test
    public void rowTest2() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8081-8085");
        conf.setString(HEARTBEAT_TIMEOUT.key(), "300000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStream<Row> dataStream = env.fromSource(new DataGeneratorSource<Row>(new GeneratorFunction<Long, Row>() {
                    @Override
                    public Row map(Long value) throws Exception {
                        return Row.of("Alice", 12);
                    }
                }, 10, Types.ROW(Types.STRING, Types.INT)),
                WatermarkStrategy.noWatermarks(), "source");

        System.out.println(dataStream.getType());
        System.out.println(TypeExtractor.getForObject(Row.of("Alice", 12)));
        System.out.println(TypeExtractor.getForObject(Row.of("Alice", 12.2)));


        dataStream.map(new LogMap<>()).disableChaining()
                .print();

        env.execute();
    }


    public static class PojoData implements Serializable {
        private long id;
        private String name;

        public PojoData(long id, String name) {
            this.id = id;
            this.name = name;
            System.out.println("PojoData(id, name) init");
        }

        public PojoData() {
            System.out.println("PojoData() init");
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "PojoData{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class ImplicitPojoData implements Serializable {
        public long id;
        public String name;
    }

    public static class NoPojoData implements Serializable {
        private long id;
        private String name;
    }

    public static class NoPojoData2 implements Serializable {
        private long id;
        private String name;
        public Map<String, Object> datas;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, Object> getDatas() {
            return datas;
        }

        public void setDatas(Map<String, Object> datas) {
            this.datas = datas;
        }
    }
}
