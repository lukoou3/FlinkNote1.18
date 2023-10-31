package com.java.flink.gene;

public class LongGeneInc extends AbstractFieldGene<Long>{
    private long start;

    public LongGeneInc(String fieldName, long start) {
        super(fieldName);
        this.start = start;
    }

    @Override
    public Long geneValue() throws Exception {
        Long rst = start;
        start++;
        return rst;
    }
}
