package com.java.flink.gene;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class StringGeneFromList extends AbstractFieldGene<String>{
    private String[] values;
    private boolean random;
    private int index = 0;

    public StringGeneFromList(String fieldName, List<String> values, boolean random) {
        super(fieldName);
        this.values = values.toArray(new String[values.size()]);
        this.random = random;
    }

    @Override
    public String geneValue() throws Exception {
        if(!random){
            if(index == values.length){
                index = 0;
            }
            return values[index++];
        }else{
            return values[ThreadLocalRandom.current().nextInt(values.length)];
        }
    }
}
