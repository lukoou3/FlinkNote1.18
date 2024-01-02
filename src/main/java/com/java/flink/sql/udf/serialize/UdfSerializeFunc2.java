package com.java.flink.sql.udf.serialize;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class UdfSerializeFunc2 extends ScalarFunction {
    static final Logger LOG = LoggerFactory.getLogger(UdfSerializeFunc2.class);
    String cache;
    @Override
    public void open(FunctionContext context) throws Exception {
        LOG.warn("open:{}.", this.hashCode());
    }

    public String eval(String a, String b){
        if(cache == null){
            LOG.warn("cache_null.cache:{}", b);
            cache = b;
        }
        return cache;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(new TypeStrategy() {
                    @Override
                    public Optional<DataType> inferType(CallContext callContext) {
                        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
                        if (argumentDataTypes.size() != 2) {
                            throw callContext.newValidationError("arg size error");
                        }
                        if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
                            throw callContext.newValidationError("Literal expected for second argument.");
                        }
                        cache = callContext.getArgumentValue(1, String.class).get();
                        return Optional.of(DataTypes.STRING());
                    }
                })
        .build();
    }
}
