package com.java.flink.sql.udaf

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MyAvgSuite extends AnyFunSuite with BeforeAndAfterAll {
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
   * window聚合不会使用TWO_PHASE，普通的会使用。其实感觉都是可以使用的
   * org.apache.flink.table.runtime.operators.aggregate.MiniBatchLocalGroupAggFunction
   * org.apache.flink.table.runtime.operators.bundle.AbstractMapBundleOperator
   *    AbstractMapBundleOperator调用MiniBatchLocalGroupAggFunction的addInput方法完成map端聚合
   *    调用finishBundle输出聚合数据，为了保证状态一直性，prepareSnapshotPreBarrier方法时(cp前)也调用了finishBundle方法，因为此Operator本身不存储状态。
   */
  test("test-agg-phase-strategy"){
    tEnv.createTemporarySystemFunction("myAvg", classOf[MyAvg])
    // access flink configuration
    val configuration = tEnv.getConfig()
    // set low-level key-value options
    configuration.set("table.exec.mini-batch.enabled", "true") // enable mini-batch optimization
    configuration.set("table.exec.mini-batch.allow-latency", "5 s") // use 5 seconds to buffer input records
    configuration.set("table.exec.mini-batch.size", "5000") // the maximum number of records can be buffered by each aggregate operator task
    configuration.set("table.optimizer.agg-phase-strategy", "TWO_PHASE")

    var sql = """
    CREATE TABLE tmp_tb (
      name string,
      age int,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
       --'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.name.expression' = '#{regexify ''(莫南){1}''}',
      -- 'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      -- 'fields.age.null-rate' = '0.2',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        window_start,
        window_end,
        name,
        sum(age) sum_age,
        -- myAvg(age) avg_age,
        count(age) cnt_age
    from table( tumble(table tmp_tb, descriptor(proctime), interval '10' second) )
    group by window_start, window_end, name
    """
    sql =
      """
      select
          name,
          sum(age) sum_age,
          myAvg(age) avg_age,
          count(age) cnt_age
      from tmp_tb
      group by name
      """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }

}
