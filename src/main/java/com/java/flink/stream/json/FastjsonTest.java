package com.java.flink.stream.json;

import com.alibaba.fastjson2.*;
import org.junit.Test;

public class FastjsonTest {

    @Test
    public void testJSONParseObject() throws Exception {
        String str = "{\"name\":\"PcLQC\",\"id\":60835108}";
        JSONObject map = JSON.parseObject(str);
        System.out.println(map);
    }

    @Test
    public void testJSONParseObjectMapReuse() throws Exception {
        String str = "{\"name\":\"PcLQC\",\"id\":60835108}";
        JSONObject map = new JSONObject();
        OptimizedJSON.parseObject(str,map);
        System.out.println(map);
        map.clear();
        System.out.println(map);
        OptimizedJSON.parseObject(str,map);
        System.out.println(map);
    }


}
