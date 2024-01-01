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

  /**
   * '#{date.past ''15'',''SECONDS''}'
   * 生成过去最多15秒的时间戳，也就是说生成的时间戳范围：[ts-15, ts]
   * '#{date.past ''15'',''5'',''SECONDS''}'
   * 生成过去最多15秒但至少5秒的时间戳，也就是说生成的时间戳范围：[ts-15, ts-5]
   * '#{date.past ''2'',''0'',''SECONDS''}',
   * 生成过去最多2秒的时间戳，也就是说生成的时间戳范围：[ts-2, ts]
   * TIMESTAMP 没有时区的时间，字符串显示的utc的时间字符串
   * TIMESTAMP_LTZ 带时区的时间，字符串显示的本地的时间字符串
   */
  test("geneTimestamp"){
    var sql = """
    CREATE TEMPORARY TABLE tmp_tb (
      `timestamp1` TIMESTAMP(3),
      `timestamp2` TIMESTAMP(3),
      `timestamp3` TIMESTAMP(3),
      `timestamp_ltz1` TIMESTAMP_LTZ (3),
      `timestamp_ltz2` TIMESTAMP_LTZ (3),
      `timestamp_ltz3` TIMESTAMP_LTZ (3),
      proc_time AS PROCTIME()
    ) WITH (
      'connector' = 'faker',
      'fields.timestamp1.expression' = '#{date.past ''15'',''SECONDS''}',
      'fields.timestamp2.expression' = '#{date.past ''15'',''5'',''SECONDS''}',
      'fields.timestamp3.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.timestamp_ltz1.expression' = '#{date.past ''15'',''SECONDS''}',
      'fields.timestamp_ltz2.expression' = '#{date.past ''15'',''5'',''SECONDS''}',
      'fields.timestamp_ltz3.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select * from tmp_tb
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }
}
