package com.java.flink.gene;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FieldGeneUtils {

    public static List<AbstractFieldGene<?>> createFieldGenesFromJson(String json){
        JSONArray jsonArray = JSON.parseArray(json);
        List<AbstractFieldGene<?>> fieldGenes = new ArrayList<>(jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String type = jsonObject.getString("type");
            JSONObject fields = jsonObject.getJSONObject("fields");
            AbstractFieldGene<?> gene = null;
            switch (type) {
                case "int_random":
                    gene = new IntGeneRandom(
                            fields.getString("name"),
                            fields.getIntValue("start", 0),
                            fields.getIntValue("end", Integer.MAX_VALUE - 1),
                            Optional.ofNullable(fields.getDouble("null_ratio")).orElse(0D)
                    );
                    break;
                case "long_inc":
                    gene = new LongGeneInc(
                            fields.getString("name"),
                            fields.getLongValue("start", 0),
                            fields.getLongValue("step", 1)
                    );
                    break;
                case "long_random":
                    gene = new LongGeneRandom(
                            fields.getString("name"),
                            fields.getLongValue("start", 0),
                            fields.getLongValue("end", Long.MAX_VALUE - 1),
                            Optional.ofNullable(fields.getDouble("null_ratio")).orElse(0D)
                    );
                    break;
                case "long_inc_batch":
                    gene = new LongGeneIncBatch(
                            fields.getString("name"),
                            fields.getLongValue("start", 0),
                            fields.getIntValue("batch", 10)
                    );
                    break;
                case "str_fix":
                    gene = new StringFixGeneRandom(
                            fields.getString("name"),
                            fields.getIntValue("len", 0)
                    );
                    break;
                case "str_from_list":
                    gene = new StringGeneFromList(
                            fields.getString("name"),
                            fields.getList("values", String.class),
                            fields.getIntValue("random", 0) == 1
                    );
                    break;
                case "str_list_from_list":
                    gene = new StringListGeneFromList(
                            fields.getString("name"),
                            fields.getList("values", String.class),
                            fields.getIntValue("minSize", 0),
                            fields.getIntValue("maxSize", 2),
                            fields.getIntValue("duplicated", 0) == 1
                    );
                    break;

                default:
                    throw new RuntimeException(type);
            }
            fieldGenes.add(gene);
        }

        return fieldGenes;
    }

}
