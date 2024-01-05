package com.java.flink.formats.json;

import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.internal.trove.map.hash.TLongIntHashMap;
import com.alibaba.fastjson2.util.Fnv;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

// org.apache.flink.formats.json.JsonRowDataDeserializationSchema
public class JsonRowDataFastJosn2SimpleDeserializationSchema implements DeserializationSchema<RowData> {
    private RowType rowType;
    private TypeInformation<RowData> resultTypeInfo;
    private boolean objectReuse = true;
    //transient private Map<Long, Integer> names;
    transient private TLongIntHashMap names;
    transient private ValueConverter[] fieldConverters;
    transient private GenericRowData nullRow;
    transient private GenericRowData row;


    public JsonRowDataFastJosn2SimpleDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean objectReuse) {
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.objectReuse = objectReuse;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        List<RowType.RowField> fields = rowType.getFields();
        nullRow = new GenericRowData(fields.size());
        if (objectReuse) {
            row = new GenericRowData(fields.size());
        }
        //names = new HashMap<>(fields.size() * 2);
        names = new TLongIntHashMap();
        for (int i = 0; i < fields.size(); i++) {
            names.put(Fnv.hashCode64(fields.get(i).getName()), i);
        }
        assert names.size() == fields.size();
        fieldConverters = getFieldConverters(fields);
    }

    private ValueConverter[] getFieldConverters(List<RowType.RowField> fields) {
        return fields.stream().map(x -> x.getType()).map(this::makeConverter).toArray(ValueConverter[]::new);
    }

    private ValueConverter makeConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case INTEGER:
                return this::convertToInteger;
            case BIGINT:
                return this::convertToLong;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }

    private Object convertToString(JSONReader reader) throws Exception {
        return StringData.fromString(reader.readString());
    }

    private Object convertToInteger(JSONReader reader) throws Exception {
        return reader.readInt32();
    }

    private Object convertToLong(JSONReader reader) throws Exception {
        return reader.readInt64();
    }

    private Object convertToFloat(JSONReader reader) throws Exception {
        return reader.readFloat();
    }

    private Object convertToDouble(JSONReader reader) throws Exception {
        return reader.readDouble();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try (JSONReader reader = JSONReader.of(message)) {
            if (!reader.nextIfMatch('{')) {
                return nullRow;
            }

            if(!objectReuse){
                row = new GenericRowData(names.size());
            }else{
                int i = 0;
                while (i < names.size()) {
                    row.setField(i, null);
                    i += 1;
                }
            }


            while (!reader.nextIfMatch('}') ){
                long jsonField = reader.readFieldNameHashCode();
                //Integer index = names.get(jsonField);
                int index = names.get(jsonField);
                //if (index != null) {
                if (index >= 0) {
                    row.setField(index, fieldConverters[index].convert(reader));
                }else{
                    reader.skipValue();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return nullRow;
        }

        return row;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(JSONReader reader) throws Exception;
    }
}
