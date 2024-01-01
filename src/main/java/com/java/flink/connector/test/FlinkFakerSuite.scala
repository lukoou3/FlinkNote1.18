package com.java.flink.connector.test

import com.java.flink.base.FlinkJavaBaseSuite

class FlinkFakerSuite extends FlinkJavaBaseSuite {
  override def parallelism: Int = 1

  test("ScanTableSource"){
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
    SELECT * FROM heros
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

  test("ScanTableSource1"){
    var sql = """
    CREATE TEMPORARY TABLE heros (
      `name` STRING,
      `name2` as concat(name, ' - ', name),
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


    // ui中并不能看到发送元素数
    sql = """
    create table tmp_tb_sink(
      `name` STRING,
      `name2` STRING,
      `power` STRING,
      `age` INT
    )
    with (
    'connector' = 'print'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    insert into tmp_tb_sink
    select * from heros
    """
    tEnv.executeSql(sql).await()

  }

  test("ScanTableSource2"){
    var sql = """
    create table `row_array_tb` (
      `id` int,
      `datas` array<row<name string,age int>>
    ) with (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''100000''}',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'sleep-per-row' = '500'
      -- 'number-of-rows' = '10'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    SELECT * FROM row_array_tb
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }
}
