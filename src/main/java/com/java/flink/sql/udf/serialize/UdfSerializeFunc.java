package com.java.flink.sql.udf.serialize;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdfSerializeFunc extends ScalarFunction {
    static final Logger LOG = LoggerFactory.getLogger(UdfSerializeFunc.class);
    String cache;
    @Override
    public void open(FunctionContext context) throws Exception {
        LOG.warn("open:{}.", this.hashCode());
    }

    public String eval(String a, String b){
        if(cache == null){
            LOG.warn("cache_null.cache:{}", b);
            cache = b;
        }
        return cache;
    }
}
