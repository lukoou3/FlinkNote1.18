package com.java.flink.connector.test;

import net.datafaker.Faker;
import org.junit.Test;

import java.util.List;

public class FakerExpressionTest {

    @Test
    public void test() throws Exception{
        Faker faker = new Faker();
        String expression = faker.expression("#{letterify 'test????test'}");
        expression = faker.expression("#{number.number_between '1','10'}");
        faker.letterify("");
        faker.name().name();
        faker.internet().emailAddress();
        faker.internet().ipV4Address();
        faker.internet().ipV6Address();
        faker.number().numberBetween(1, 100);
        faker.regexify("[a-z]{4,10}");
    }

    @Test
    public void test2() throws Exception{
        Faker faker = new Faker();
        System.out.println(faker.expression("#{internet.emailAddress}"));
        System.out.println(faker.expression("#{internet.ipV4Address}"));
        System.out.println(faker.expression("#{internet.ipV6Address}"));
        System.out.println(faker.expression("#{phoneNumber.phoneNumber}"));
        System.out.println(faker.expression("#{phoneNumber.cellPhone}"));
    }

    @Test
    public void testCollection() throws Exception{
        Faker faker = new Faker();
        List<Object> objects =
                faker.<Object>collection(
                                () -> faker.name().firstName(),
                                () -> faker.random().nextInt(100))
                        .nullRate(0.3)
                        .maxLen(10)
                        .generate();
        System.out.println(objects);
    }

}
