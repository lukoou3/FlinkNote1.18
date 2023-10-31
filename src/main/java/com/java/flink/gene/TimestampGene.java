package com.java.flink.gene;

public class TimestampGene extends AbstractFieldGene<Long>{
    public TimestampGene(String fieldName) {
        super(fieldName);
    }

    @Override
    public Long geneValue() throws Exception {
        return System.currentTimeMillis();
    }
}
