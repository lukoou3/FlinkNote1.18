package com.java.flink.stream.theory;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.junit.Test;

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
        assert serializer.copy(str) == str;
        assert serializer.getClass() == StringSerializer.class;
        assert serializer == StringSerializer.INSTANCE;
    }

}
