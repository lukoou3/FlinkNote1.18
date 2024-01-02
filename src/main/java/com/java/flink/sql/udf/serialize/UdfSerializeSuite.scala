package com.java.flink.sql.udf.serialize

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
class UdfSerializeSuite extends AnyFunSuite with BeforeAndAfterAll{
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()

    tEnv = StreamTableEnvironment.create(env)
  }

  /**
   * 2个task，只是每个task有一个udf，udf_ser(name, 'a')和udf_ser(name, 'b')没区分开
   * 它这函数的序列化真傻屌，单个task的2个udf_ser序列化后还是同一个对象，不是2个
   * getTypeInference中修改udf的属性可以实现2个不同的对象
   * 都1.18了还是这样，难道被人都没遇到这个问题反馈?
   */
  test("UdfSerializeFunc"){
    tEnv.createTemporarySystemFunction("udf_ser", classOf[UdfSerializeFunc])

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
    select
        name,
        udf_ser(name, 'a') name1,
        udf_ser(name, 'b') name2
    from heros
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

  /**
   * 修改ScalarFunction的属性，能使之序列化后是不同的对象
   */
  test("UdfSerializeFunc2"){
    tEnv.createTemporarySystemFunction("udf_ser", classOf[UdfSerializeFunc2])

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
    select
        name,
        udf_ser(name, 'a') name1,
        udf_ser(name, 'b') name2
    from heros
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }

}
