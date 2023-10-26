package com.java.flink.stream.theory;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;

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
        ((PojoSerializer<PojoData>)serializer).serialize(data, dataOutput);
        readBuffer = dataOutput.wrapAsByteBuffer();

        dataInput = new DataInputDeserializer(readBuffer.duplicate());
        data2 = ((PojoSerializer<PojoData>)serializer).deserialize(dataInput);
        System.out.println(data2);
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
}
