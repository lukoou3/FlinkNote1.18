package com.java.flink.stream.theory;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.MathUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// 之前写的生成使keyBy后下游均匀分布的key，仅做查看参考，在其他文件测试hash选择下游index
public class TaskIdxUtils {

    private static int getIndex(Object key, int parallelism){
        return (MathUtils.murmurHash(key.hashCode()) % 128) * parallelism / 128;
    }

    public static int[] getMeanIndexs(int parallelism) {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 1; i < 1000; i++) {
            int id = getIndex(i, parallelism);
            if (!map.containsKey(id)) {
                map.put(id, i);
            }
        }
        assert map.size() == parallelism;
        int[] idx = new int[parallelism];
        int i = 0;
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            idx[i] = entry.getValue();
            i++;
        }
        return idx;
    }

    public static int[] getMeanIndexsForTuple4(Long vsys_id, String device_group , String data_center, int parallelism) {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 1; i < 1000; i++) {
            Tuple4<Long, String, String, Integer> key = new Tuple4<>(vsys_id, device_group, data_center, i);
            int id = getIndex(key, parallelism);
            if (!map.containsKey(id)) {
                map.put(id, i);
            }
            if(map.size() == parallelism){
                break;
            }
        }
        assert map.size() == parallelism;
        int[] idx = new int[parallelism];
        int i = 0;
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            idx[i] = entry.getValue();
            i++;
        }
        return idx;
    }

    public static int[] getMeanIndexsForTuple5(Long vsys_id, String device_group , String data_center, long ts, int parallelism) {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 1; i < 1000; i++) {
            Tuple5<Long, String, String, Long,Integer> key = new Tuple5<>(vsys_id, device_group, data_center, ts, i);
            int id = getIndex(key, parallelism);
            if (!map.containsKey(id)) {
                map.put(id, i);
            }
            if(map.size() == parallelism){
                break;
            }
        }
        assert map.size() == parallelism;
        int[] idx = new int[parallelism];
        int i = 0;
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            idx[i] = entry.getValue();
            i++;
        }
        return idx;
    }

    public static void main(String[] args) {
        for (int index : getMeanIndexs(24)) {
            System.out.print(index);
            System.out.print(", ");
        }

    }

    public static class IdxAndRand implements Serializable {
        public int[] idxs;
        public int rand;

        public IdxAndRand(int[] idxs, int rand) {
            this.idxs = idxs;
            this.rand = rand;
        }

        public IdxAndRand() {
        }
    }
}
