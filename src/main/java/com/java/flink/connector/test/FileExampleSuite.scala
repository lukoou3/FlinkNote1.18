package com.java.flink.connector.test

import com.java.flink.base.FlinkJavaBaseSuite

class FileExampleSuite extends FlinkJavaBaseSuite {
  override def parallelism: Int = 1

  test("FileExampleSource"){
    var sql = """
    CREATE TEMPORARY TABLE files (
      `line` STRING
    ) WITH (
      'connector' = 'file_example',
      'path' = 'D:\doc\logs'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    SELECT * FROM files
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }


}
