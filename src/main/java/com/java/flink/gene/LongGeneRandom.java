package com.java.flink.gene;

import java.util.concurrent.ThreadLocalRandom;

public class LongGeneRandom extends AbstractFieldGene<Long>{
    private long start;
    private long end;
    private double nullRatio;
    private boolean nullAble;

    public LongGeneRandom(String fieldName, long start, long end, double nullRatio) {
        super(fieldName);
        this.start = start;
        this.end = end;
        this.nullRatio = nullRatio;
        this.nullAble = nullRatio > 0D;
    }

    @Override
    public Long geneValue() throws Exception {
        if(nullAble && ThreadLocalRandom.current().nextDouble() < nullRatio){
            return null;
        }else{
            return ThreadLocalRandom.current().nextLong(start, end);
        }
    }
}
