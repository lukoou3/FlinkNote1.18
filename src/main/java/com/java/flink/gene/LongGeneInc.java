package com.java.flink.gene;

public class LongGeneInc extends AbstractFieldGene<Long>{
    private long start;
    private long step;

    public LongGeneInc(String fieldName, long start) {
        this(fieldName, start, 1);
    }

    public LongGeneInc(String fieldName, long start, long step) {
        super(fieldName);
        this.start = start;
        this.step = step;
    }

    @Override
    public Long geneValue() throws Exception {
        Long rst = start;
        start += step;
        return rst;
    }
}
