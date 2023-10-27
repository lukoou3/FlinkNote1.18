package com.java.flink.stream.theory;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.MathUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * flink分区算子：
 * https://zhuanlan.zhihu.com/p/546827671
 * https://blog.csdn.net/asd491310/article/details/120393146
 *
 * key by算子的分区器：org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner
 * KeyGroupStreamPartitioner选择下游Channel的调用的是selectChannel方法，逻辑：
 *      1、keySelector.getKey(value) 获取分区key
 *      2、KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels); 计算Channel
 *      3、(MathUtils.murmurHash(key.hashCode()) % maxParallelism) * parallelism / maxParallelism，范围是[0, 1) * parallelism, maxParallelism默认是128
 *          相当于把key分成128份，然后再归到对应的parallelism，为啥不直接hash % parallelism呢
 */
public class KeyGroupStreamPartitionerTest {

    private static int getDefaultIndex(Object key, int parallelism){
        return getIndex(key, parallelism, 128);
    }
    private static int getIndex(Object key, int parallelism, int maxParallelism){
        return (MathUtils.murmurHash(key.hashCode()) % maxParallelism) * parallelism / maxParallelism;
    }

    @Test
    public void testSelectChannel() throws Exception {
        int parallelism = 8;
        for (int i = 1; i < 10000; i++) {
            Integer key = i;
            int index = getDefaultIndex(key, parallelism);
            int index2 = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, 128, parallelism);
            Assert.assertTrue("index not ==", index == index2);
            assert index == index2: "index not ==";
        }
    }


    public static void main(String[] args) throws Exception {

    }

}
