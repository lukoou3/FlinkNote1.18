package com.java.flink.connector.test

import com.alibaba.fastjson2.JSON
import com.java.flink.base.FlinkJavaBaseSuite
import com.java.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcPojoOptions, PojoBatchIntervalJdbcSink}
import com.java.flink.connector.test.PojoBatchIntervalJdbcSinkSuite.People
import com.java.flink.util.function.{SerializableFunction, SerializablePredicate}
import org.apache.flink.streaming.api.datastream.DataStream

import java.util
import scala.beans.BeanProperty

class PojoBatchIntervalJdbcSinkSuite extends FlinkJavaBaseSuite {
  override def parallelism: Int = 1

  test("upsert"){
    /**
     * {"code":"1", "name":"莫南", "age":18}
     * {"code":"1", "name":"莫南", "age":19}
     * {"code":"1", "name":"莫南", "age":18, "birthday":"2000-01-01 00:00:10"}
     * {"code":"2", "name":"燕青丝", "age":17, "birthday":"2000-01-01 00:00:10"}
     *
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    val ds: DataStream[People] = env.socketTextStream("localhost", 9999)
      .map(x => JSON.parseObject(x, classOf[People]))

    val sink = new PojoBatchIntervalJdbcSink[People](
      JdbcConnectionOptions.builder()
        .withUrl("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8")
        .withUsername("root")
        .withPassword("123456")
        .build(),
      JdbcExecutionOptions.builder()
        .withBatchSize(5)
        .withBatchIntervalMs(3000)
        .build(),
      JdbcPojoOptions.builder[People]()
        .withClass(classOf[People])
        .withTableName("people")
        .withUpdateMode(true)
        .build()
    )
    ds.addSink(sink)

  }

  test("upsert_with_has_delete"){
    /**
     * {"code":"1", "name":"莫南", "age":18}
     * {"code":"1", "name":"莫南", "age":19}
     * {"code":"1", "name":"莫南", "age":18, "birthday":"2000-01-01 00:00:10"}
     * {"code":"2", "name":"燕青丝", "age":17, "birthday":"2000-01-01 00:00:10"}
     *
     * {"code":"1", "age":0}
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    val ds: DataStream[People] = env.socketTextStream("localhost", 9999)
      .filter(_.trim.nonEmpty)
      .map(x => JSON.parseObject(x, classOf[People]))

    val sink = new PojoBatchIntervalJdbcSink[People](
      JdbcConnectionOptions.builder()
        .withUrl("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8")
        .withUsername("root")
        .withPassword("123456")
        .build(),
      JdbcExecutionOptions.builder()
        .withBatchSize(5)
        .withBatchIntervalMs(3000)
        .build(),
      JdbcPojoOptions.builder[People]()
        .withClass(classOf[People])
        .withTableName("people")
        .withUpdateMode(true)
        .withKeyedMode(true)
        .withKeyExtractor(new SerializableFunction[People, Object] {
          override def apply(t: People): Object = t.code.asInstanceOf[Integer]
        })
        .withHasDelete(true)
        .withDeleteKeyFields(util.Arrays.asList("code"))
        .withDeleteDaTaPredicate(new SerializablePredicate[People] {
          override def test(t: People): Boolean = t.age == 0
        })
        .build()
    )
    ds.addSink(sink)

  }

}

object PojoBatchIntervalJdbcSinkSuite{
  class People {
    @BeanProperty
    var code: Int = _
    @BeanProperty
    var name: String = _
    @BeanProperty
    var age: java.lang.Integer = _
    @BeanProperty
    var birthday: String = _
  }
}