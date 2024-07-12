package com.java.flink.sql.internal.parse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.planner.delegation.StreamPlanner;

public class ExpressionParserTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env);
        StreamPlanner streamPlanner = (StreamPlanner) tEnv.getPlanner();
        CatalogManager catalogManager = tEnv.getCatalogManager();
        OperationTreeBuilder operationTreeBuilder = tEnv.getOperationTreeBuilder();
        //PlannerExpressionParser.create();
        tEnv.fromTableSource(null).select();
    }

}
