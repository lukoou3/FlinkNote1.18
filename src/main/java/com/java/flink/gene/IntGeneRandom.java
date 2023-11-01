package com.java.flink.gene;

import java.util.concurrent.ThreadLocalRandom;

public class IntGeneRandom extends AbstractFieldGene<Integer>{
    private int start;
    private int end;
    private double nullRatio;
    private boolean nullAble;
    private boolean oneValue;
    public IntGeneRandom(String fieldName, int start, int end, double nullRatio) {
        super(fieldName);
        this.start = start;
        this.end = end + 1;
        this.nullRatio = nullRatio;
        this.nullAble = nullRatio > 0D;
        this.oneValue = start == end;
    }

    @Override
    public Integer geneValue() throws Exception {
        if(nullAble && ThreadLocalRandom.current().nextDouble() < nullRatio){
            return null;
        }else{
            if(oneValue){
                return start;
            }
            return ThreadLocalRandom.current().nextInt(start, end);
        }
    }
}
