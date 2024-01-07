package com.java.flink.util;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.Arrays;

public class JavaReflection {
    public static <T> PropertyDescriptor[] getJavaBeanReadableProperties(Class<T> beanClass) throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(beanClass);
        return Arrays.stream(beanInfo.getPropertyDescriptors())
                .filter(p -> !"class".equals(p.getName()) && !"declaringClass".equals(p.getName()) && p.getReadMethod() != null)
                .toArray(PropertyDescriptor[]::new);
    }

    public static <T> PropertyDescriptor[] getJavaBeanReadableAndWritableProperties(Class<T> beanClass) throws IntrospectionException {
        return Arrays.stream(getJavaBeanReadableProperties(beanClass))
                .filter(p -> p.getWriteMethod() != null)
                .toArray(PropertyDescriptor[]::new);
    }
}
