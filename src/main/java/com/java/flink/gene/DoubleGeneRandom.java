package com.java.flink.gene;

import java.util.concurrent.ThreadLocalRandom;

public class DoubleGeneRandom extends AbstractFieldGene<Double>{
    private double start;
    private double end;
    private double nullRatio;
    private boolean nullAble;

    public DoubleGeneRandom(String fieldName, double start, double end, double nullRatio) {
        super(fieldName);
        this.start = start;
        this.end = end;
        this.nullRatio = nullRatio;
        this.nullAble = nullRatio > 0D;
    }

    @Override
    public Double geneValue() throws Exception {
        if(nullAble && ThreadLocalRandom.current().nextDouble() < nullRatio){
            return null;
        }else{
            return ThreadLocalRandom.current().nextDouble(start, end);
        }
    }
}
