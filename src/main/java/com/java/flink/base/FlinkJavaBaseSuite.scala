package com.java.flink.base

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

abstract class FlinkJavaBaseSuite extends AnyFunSuite with BeforeAndAfterAll {
    var env: StreamExecutionEnvironment = _
    var tEnv: StreamTableEnvironment = _
    var execute = true

    def parallelism: Int

    override protected def beforeAll(): Unit = {
        val conf = new Configuration()
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        env.setParallelism(parallelism)
        env.getConfig.enableObjectReuse()

        // Flink 1.14后，旧的planner被移除了，默认就是BlinkPlanner
        //val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
        //tEnv = StreamTableEnvironment.create(env, settings)
        // 其实直接这样就行
        tEnv = StreamTableEnvironment.create(env)
    }

    override protected def afterAll(): Unit = {
        if(execute){
            env.execute()
        }
    }
}
