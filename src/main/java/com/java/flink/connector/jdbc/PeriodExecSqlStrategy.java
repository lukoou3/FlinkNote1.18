package com.java.flink.connector.jdbc;

import java.io.Serializable;
import java.util.List;

public interface PeriodExecSqlStrategy extends Serializable {
    List<String> sqlsThisTime(long ts);
}
