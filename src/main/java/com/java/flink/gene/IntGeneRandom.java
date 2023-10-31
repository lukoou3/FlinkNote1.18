package com.java.flink.gene;

import java.util.concurrent.ThreadLocalRandom;

public class IntGeneRandom extends AbstractFieldGene<Integer>{
    private int start;
    private int end;
    private double nullRatio;
    private boolean nullAble;

    public IntGeneRandom(String fieldName, int start, int end, double nullRatio) {
        super(fieldName);
        this.start = start;
        this.end = end;
        this.nullRatio = nullRatio;
        this.nullAble = nullRatio > 0D;
    }

    @Override
    public Integer geneValue() throws Exception {
        if(nullAble && ThreadLocalRandom.current().nextDouble() < nullRatio){
            return null;
        }else{
            return ThreadLocalRandom.current().nextInt(start, end);
        }
    }
}
