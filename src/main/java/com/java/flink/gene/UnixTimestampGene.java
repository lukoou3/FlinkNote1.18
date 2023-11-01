package com.java.flink.gene;

public class UnixTimestampGene extends AbstractFieldGene<Long>{
    public UnixTimestampGene(String fieldName) {
        super(fieldName);
    }

    @Override
    public Long geneValue() throws Exception {
        return System.currentTimeMillis() / 1000;
    }
}
