package com.java.flink.stream.func;

import com.alibaba.fastjson2.JSON;
import com.java.flink.gene.AbstractFieldGene;
import com.java.flink.gene.FieldGeneUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.List;

public class FieldGeneSouce extends RichParallelSourceFunction<String> {
    private volatile boolean stop;
    private int indexOfSubtask;
    private List<AbstractFieldGene<?>> fieldGenes;
    private int speed;
    private int speedUnit;

    public FieldGeneSouce(String fieldGenesDesc) {
        this(fieldGenesDesc, 1);
    }

    public FieldGeneSouce(String fieldGenesDesc, int speed) {
        this(fieldGenesDesc, speed, 1000);
    }

    public FieldGeneSouce(String fieldGenesDesc, int speed, int speedUnit) {
        this.fieldGenes = FieldGeneUtils.createFieldGenesFromJson(fieldGenesDesc);
        this.speed = speed;
        this.speedUnit = speedUnit;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
        for (int i = 0; i < fieldGenes.size(); i++) {
            fieldGenes.get(i).open();
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long start;
        AbstractFieldGene fieldGene;
        Object value;
        HashMap<String, Object> map = new HashMap<>(fieldGenes.size() * 2);
        while (!stop) {
            start = System.currentTimeMillis();

            for (int i = 0; i < speed; i++) {
                map.clear();
                for (int j = 0; j < fieldGenes.size(); j++) {
                    fieldGene = fieldGenes.get(j);
                    value = fieldGene.geneValue();
                    if(value != null){
                        map.put(fieldGene.fieldName(), value);
                    }
                }

                String ele = JSON.toJSONString(map);
                ctx.collect(ele);
            }

            long t = System.currentTimeMillis() - start;
            if(t < speedUnit){
                Thread.sleep(speedUnit - t);
            }
        }
    }

    @Override
    public void close() throws Exception {
        for (int i = 0; i < fieldGenes.size(); i++) {
            fieldGenes.get(i).close();
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
