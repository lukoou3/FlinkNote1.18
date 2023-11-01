package com.java.flink.gene;

import java.util.concurrent.ThreadLocalRandom;

public class LongGeneRandom extends AbstractFieldGene<Long>{
    private long start;
    private long end;
    private double nullRatio;
    private boolean nullAble;
    private boolean oneValue;

    public LongGeneRandom(String fieldName, long start, long end, double nullRatio) {
        super(fieldName);
        this.start = start;
        this.end = end + 1;
        this.nullRatio = nullRatio;
        this.nullAble = nullRatio > 0D;
        this.oneValue = start == end;
    }

    @Override
    public Long geneValue() throws Exception {
        if(nullAble && ThreadLocalRandom.current().nextDouble() < nullRatio){
            return null;
        }else{
            if(oneValue){
                return start;
            }
            return ThreadLocalRandom.current().nextLong(start, end);
        }
    }
}
