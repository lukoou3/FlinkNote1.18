package com.java.flink.gene;

public class LongGeneIncBatch extends AbstractFieldGene<Long>{
    private long start;
    private int batch;
    private int cnt;

    public LongGeneIncBatch(String fieldName, long start, int batch) {
        super(fieldName);
        this.start = start;
        this.batch = batch;
    }

    @Override
    public Long geneValue() throws Exception {
        Long rst = start;
        cnt++;
        if(cnt == batch){
            cnt = 0;
            start++;
        }
        return rst;
    }
}
