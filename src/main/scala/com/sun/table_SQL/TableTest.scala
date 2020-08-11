package com.sun.table_SQL

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/10 20:26
 * @description： TODO
 * @version: 1.0
 */
object TableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    )
    //基于流式数据创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //基于流创建表
    val dataTable: Table = tableEnv.fromDataStream(sourceDS)
    //调用table api 进行砖汉
    val selectTable: Table = dataTable.select("id,temperature").filter("id=='sensor_1'")
    //selectTable.toAppendStream[(String,Double)].print("selectTableResult")
    //直接使用sql      selectview 检查点路径 是表名
    tableEnv.createTemporaryView("selectview ", selectTable)
    val sql = "select id,temperature from selectview where id='sensor_1'"
    val resultSqlTable: Table = tableEnv.sqlQuery(sql)
    resultSqlTable.toAppendStream[(String, Double)].print("sql")

    env.execute("table, test")
  }

}
