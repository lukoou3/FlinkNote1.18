package com.java.flink.gene;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class StringListGeneFromList extends AbstractFieldGene<List<String>>{
    private String[] values;
    private int minSize;
    private int maxSize;
    private boolean duplicated;

    public StringListGeneFromList(String fieldName, List<String> values, int minSize, int maxSize, boolean duplicated) {
        super(fieldName);
        this.values = values.toArray(new String[values.size()]);
        this.minSize = Math.max(minSize, 0);
        this.maxSize = duplicated? maxSize : Math.min(maxSize, values.size());
        this.duplicated = duplicated;
    }

    @Override
    public List<String> geneValue() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int size = random.nextInt(minSize, maxSize + 1);
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if(duplicated){
                list.add(values[random.nextInt(values.length)]);
            }else{
                while (true){
                    String value = values[random.nextInt(values.length)];
                    if(list.contains(value)){
                        continue;
                    }
                    list.add(value);
                    break;
                }
            }
        }
        return list;
    }
}
