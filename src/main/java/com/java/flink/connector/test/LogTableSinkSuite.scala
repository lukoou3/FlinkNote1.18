package com.java.flink.connector.test

import com.java.flink.base.FlinkJavaBaseSuite

class LogTableSinkSuite extends FlinkJavaBaseSuite {
  override def parallelism: Int = 2

  test("LogTableSink") {
    var sql = """
    CREATE TEMPORARY TABLE heros (
      `name` STRING,
      `power` STRING,
      `age` INT
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.power.expression' = '#{superhero.power}',
      'fields.power.null-rate' = '0.05',
      'rows-per-second' = '1',
      'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    create table tmp_tb_sink(
      `name` STRING,
      `power` STRING,
      `age` INT
    )
    with (
    'connector' = 'log',
    'log-mode' = 'log_info',
    'format' = 'json'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    insert into tmp_tb_sink
    select * from heros
    """
    tEnv.executeSql(sql).await()

  }

}
