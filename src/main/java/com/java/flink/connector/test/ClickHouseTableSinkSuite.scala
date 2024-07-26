package com.java.flink.connector.test

import com.java.flink.base.FlinkJavaBaseSuite

/**
CREATE TABLE IF NOT EXISTS test.test_ck_sink_local (
    id Int64,
    datetime DateTime,
    datetime64 DateTime64(3),
    datetime_2 DateTime,
    datetime64_2 DateTime64(3),
    insert_time Int64 MATERIALIZED toUnixTimestamp(now()),
    int64 Int64,
    uint64 UInt64,
    int32 Int32,
    uint32 UInt32,
    int32_nullalbe Nullable(Int32),
    str String,
    str_dict_encoded LowCardinality(String),
    int32_list Array(Int32),
    int64_list Array(Int64),
    str_list Array(String)
)
ENGINE=MergeTree
PARTITION BY toYYYYMMDD(datetime)
ORDER BY (datetime, id)
;

 */
class ClickHouseTableSinkSuite extends FlinkJavaBaseSuite {
  override def parallelism: Int = 1

  test("ClickHouseSink1") {
    var sql = """
    create temporary table tmp_source (
      `id` int,
      `datetime` TIMESTAMP(3),
      `datetime64` TIMESTAMP_LTZ(3),
      `datetime_2` bigint,
      `datetime64_2` bigint,
      `int64` bigint,
      `uint64` bigint,
      `int32` int,
      `uint32` bigint,
      `int32_nullalbe` int,
      `str` string,
      `str_dict_encoded` string,
      `int32_list` array<int>,
      `int64_list` array<bigint>,
      `str_list` array<string>
    ) with (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.datetime.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime64.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.datetime64_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.int64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.uint64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.uint32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32_nullalbe.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.str.expression' = '#{superhero.name}',
      'fields.str_dict_encoded.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.int32_list.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.int32_list.length' = '3',
      'fields.int64_list.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int64_list.length' = '3',
      'fields.str_list.expression' = '#{regexify ''[a-z]{3,5}''}',
      'fields.str_list.length' = '3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    create table tmp_sink(
      `id` int,
      `datetime` TIMESTAMP(3),
      `datetime64` TIMESTAMP_LTZ(3),
      `datetime_2` bigint,
      `datetime64_2` bigint,
      `int64` bigint,
      `uint64` bigint,
      `int32` int,
      `uint32` bigint,
      `int32_nullalbe` int,
      `str` string,
      `str_dict_encoded` string,
      `int32_list` array<int>,
      `int64_list` array<bigint>,
      `str_list` array<string>
    )
    with (
    'connector' = 'clickhouse',
    'table' = 'test.test_ck_sink_local',
    'host' = '192.168.40.223:9001',
    'batch.interval' = '10s',
    'connection.user' = 'default',
    'connection.password' = 'galaxy2019'
    )
    """
    tEnv.executeSql(sql)

    /*sql = """
    select * from tmp_source
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    rstTable.execute().print()*/

    sql = """
    insert into tmp_sink
    select * from tmp_source
    """
    tEnv.executeSql(sql).await()
  }

  test("ClickHouseSink2") {
    var sql = """
    create temporary table tmp_source (
      `id` int,
      `datetime` TIMESTAMP(3),
      `datetime64` TIMESTAMP_LTZ(3),
      `datetime_2` bigint,
      `datetime64_2` bigint,
      `int64` bigint,
      `uint64` bigint,
      `int32` int,
      `uint32` bigint,
      `int32_nullalbe` int,
      `str` string,
      `str_dict_encoded` string,
      `int32_list` array<int>,
      `int64_list` array<bigint>,
      `str_list` array<string>
    ) with (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.datetime.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime.null-rate' = '0.5',
      'fields.datetime64.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime64.null-rate' = '0.5',
      'fields.datetime_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.datetime64_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.int64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int64.null-rate' = '0.5',
      'fields.uint64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.uint64.null-rate' = '0.5',
      'fields.int32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32.null-rate' = '0.5',
      'fields.uint32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.uint32.null-rate' = '0.5',
      'fields.int32_nullalbe.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32_nullalbe.null-rate' = '0.5',
      'fields.str.expression' = '#{superhero.name}',
      'fields.str.null-rate' = '0.5',
      'fields.str_dict_encoded.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.str_dict_encoded.null-rate' = '0.5',
      'fields.int32_list.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.int32_list.length' = '3',
      'fields.int32_list.null-rate' = '0.5',
      'fields.int64_list.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int64_list.length' = '3',
      'fields.int64_list.null-rate' = '0.5',
      'fields.str_list.expression' = '#{regexify ''[a-z]{3,5}''}',
      'fields.str_list.length' = '3',
      'fields.str_list.null-rate' = '0.5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    create table tmp_sink(
      `id` int,
      `datetime` TIMESTAMP(3),
      `datetime64` TIMESTAMP_LTZ(3),
      `datetime_2` bigint,
      `datetime64_2` bigint,
      `int64` bigint,
      `uint64` bigint,
      `int32` int,
      `uint32` bigint,
      `int32_nullalbe` int,
      `str` string,
      `str_dict_encoded` string,
      `int32_list` array<int>,
      `int64_list` array<bigint>,
      `str_list` array<string>
    )
    with (
    'connector' = 'clickhouse',
    'table' = 'test.test_ck_sink_local',
    'host' = '192.168.40.223:9001',
    'batch.interval' = '10s',
    'connection.user' = 'default',
    'connection.password' = 'galaxy2019'
    )
    """
    tEnv.executeSql(sql)

    /*sql = """
    select * from tmp_source
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    rstTable.execute().print()*/

    sql = """
    insert into tmp_sink
    select * from tmp_source
    """
    tEnv.executeSql(sql).await()
  }

  test("ClickHouseSink3") {
    var sql = """
    create temporary table tmp_source (
      `id` int,
      `int64` bigint,
      `int32` int,
      `int32_nullalbe` int,
      `datetime` TIMESTAMP(3),
       `datetime_2` bigint,
      `str_dict_encoded` string,
      `int32_list` array<int>,
      `str_list` array<string>
    ) with (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.datetime.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.int64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int64.null-rate' = '0.5',
      'fields.int32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32.null-rate' = '0.5',
      'fields.int32_nullalbe.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32_nullalbe.null-rate' = '0.5',
      'fields.str_dict_encoded.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.str_dict_encoded.null-rate' = '0.5',
      'fields.int32_list.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.int32_list.length' = '3',
      'fields.int32_list.null-rate' = '0.5',
      'fields.str_list.expression' = '#{regexify ''[a-z]{3,5}''}',
      'fields.str_list.length' = '3',
      'fields.str_list.null-rate' = '0.5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    create table tmp_sink(
    `id` int,
    `int64` bigint,
    `int32` int,
    `int32_nullalbe` int,
    `datetime` TIMESTAMP(3),
     `datetime_2` bigint,
    `str_dict_encoded` string,
    `int32_list` array<int>,
    `str_list` array<string>
    )
    with (
    'connector' = 'clickhouse',
    'table' = 'test.test_ck_sink_local',
    'host' = '192.168.40.223:9001',
    'batch.interval' = '10s',
    'connection.user' = 'default',
    'connection.password' = 'galaxy2019'
    )
    """
    tEnv.executeSql(sql)

    /*sql = """
    select * from tmp_source
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    rstTable.execute().print()*/

    sql = """
    insert into tmp_sink
    select * from tmp_source
    """
    tEnv.executeSql(sql).await()
  }

  test("ClickHouseSinkUnknownCol") {
    var sql = """
    create temporary table tmp_source (
      `id` int,
      `datetime` TIMESTAMP(3),
      `datetime_2` bigint,
      `int64` bigint,
      `int32` int,
      `int32_nullalbe_2` int,
      `str_dict_encoded` string,
      `int32_list` array<int>,
      `str_list` array<string>
    ) with (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.datetime.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.int64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int64.null-rate' = '0.5',
      'fields.int32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32.null-rate' = '0.5',
      'fields.int32_nullalbe_2.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32_nullalbe_2.null-rate' = '0.5',
      'fields.str_dict_encoded.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.str_dict_encoded.null-rate' = '0.5',
      'fields.int32_list.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.int32_list.length' = '3',
      'fields.int32_list.null-rate' = '0.5',
      'fields.str_list.expression' = '#{regexify ''[a-z]{3,5}''}',
      'fields.str_list.length' = '3',
      'fields.str_list.null-rate' = '0.5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    create table tmp_sink(
      `id` int,
      `datetime` TIMESTAMP(3),
      `datetime_2` bigint,
      `int64` bigint,
      `int32` int,
      `int32_nullalbe_2` int,
      `str_dict_encoded` string,
      `int32_list` array<int>,
      `str_list` array<string>
    )
    with (
    'connector' = 'clickhouse',
    'table' = 'test.test_ck_sink_local',
    'host' = '192.168.40.223:9001',
    'batch.interval' = '10s',
    'connection.user' = 'default',
    'connection.password' = 'galaxy2019'
    )
    """
    tEnv.executeSql(sql)

    /*sql = """
    select * from tmp_source
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    rstTable.execute().print()*/

    sql = """
    insert into tmp_sink
    select * from tmp_source
    """
    tEnv.executeSql(sql).await()
  }

  test("ClickHouseSinkTypeCast") {
    var sql = """
    create temporary table tmp_source (
      `id` int,
      `datetime` string,
      `datetime64` string,
      `datetime_2` bigint,
      `datetime64_2` bigint,
      `int64` string,
      `uint64` string,
      `int32` string,
      `uint32` string,
      `int32_nullalbe` string,
      `str` int,
      `str_dict_encoded` bigint,
      `int32_list` array<string>,
      `int64_list` array<string>,
      `str_list` array<bigint>
    ) with (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.datetime.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime64.expression' = '#{date.past ''2'',''0'',''SECONDS''}',
      'fields.datetime_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.datetime64_2.expression' = '#{number.numberBetween ''1721036099379'',''1721036159379''}',
      'fields.int64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.uint64.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.uint32.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int32_nullalbe.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.str.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.str_dict_encoded.expression' = '#{number.numberBetween ''1'',''5''}',
      'fields.int32_list.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.int32_list.length' = '3',
      'fields.int64_list.expression' = '#{number.numberBetween ''0'',''1000000''}',
      'fields.int64_list.length' = '3',
      'fields.str_list.expression' = '#{regexify ''[0-9]{3,5}''}',
      'fields.str_list.length' = '3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    create table tmp_sink(
      `id` INT,
      `datetime` STRING,
      `datetime64` STRING,
      `datetime_2` bigint,
      `datetime64_2` bigint,
      `int64` STRING,
      `uint64` STRING,
      `int32` STRING,
      `uint32` STRING,
      `int32_nullalbe` STRING,
      `str` INT,
      `str_dict_encoded` BIGINT,
      `int32_list` ARRAY<STRING>,
      `int64_list` ARRAY<STRING>,
      `str_list` ARRAY<BIGINT>
    )
    with (
    'connector' = 'clickhouse',
    'table' = 'test.test_ck_sink_local',
    'host' = '192.168.40.223:9001',
    'batch.interval' = '10s',
    'connection.user' = 'default',
    'connection.password' = 'galaxy2019'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    create table tmp_sink2(
      `id` INT,
      `datetime` STRING,
      `datetime64` STRING,
      `datetime_2` STRING,
      `datetime64_2` STRING,
      `int64` STRING,
      `uint64` STRING,
      `int32` STRING,
      `uint32` STRING,
      `int32_nullalbe` STRING,
      `str` INT,
      `str_dict_encoded` BIGINT,
      `int32_list` ARRAY<STRING>,
      `int64_list` ARRAY<STRING>,
      `str_list` ARRAY<BIGINT>
    )
    with (
    'connector' = 'log',
    'log-mode' = 'log_info',
    'format' = 'json'
    )
    """
    tEnv.executeSql(sql)

    /*sql = """
    select * from tmp_source
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    rstTable.execute().print()*/


    sql = """
    insert into tmp_sink
    select * from tmp_source
    """
    tEnv.executeSql(sql).await()
  }
}
