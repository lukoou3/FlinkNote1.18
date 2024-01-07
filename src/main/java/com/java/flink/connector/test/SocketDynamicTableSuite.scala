package com.java.flink.connector.test

import com.java.flink.base.FlinkJavaBaseSuite

class SocketDynamicTableSuite extends FlinkJavaBaseSuite{
  override def parallelism: Int = 1

  test("socket_example"){
    /**
     * {"id":"1","name":"罗隐32","age":1300}
     * {"id":"1", "name":"罗隐", "age":30}
     * 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
     */
    var sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int
      -- PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
      'connector' = 'socket_example',
      'hostname' = 'localhost',
      'port' = '9999',
      'format' = 'json',
      -- format的参数配置，前面需要加format的名称
      'json.fail-on-missing-field' = 'false',
      -- json解析报错会直接返回null(row是null), 没法跳过忽略, {}不会报错, 属性都是null
      'json.ignore-parse-errors' = 'true'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id,
        name,
        age
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

}
