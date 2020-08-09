package com.sun.sink


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sun.apitest.SourceReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/7 23:52
 * @description： TODO
 * @version: 1.0
 */
object JdbcSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    )
    sourceDS.addSink(new MyJdbcSinkFunction)

    env.execute("jdbc sink")
  }

}

class MyJdbcSinkFunction() extends RichSinkFunction[SourceReading] {


  // 定义连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updataStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop202:3306/test", "root", "000000")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temperature) values (?, ?)")
    updataStmt = conn.prepareStatement("update sensor_temp set temperature = ? where id = ?")
  }

  override def invoke(value: SourceReading): Unit = {
    //先执行更新操作，查到就更新
    updataStmt.setDouble(1, value.temperature)
    updataStmt.setString(2, value.id)
    updataStmt.execute()
    //如果更新没有查到数据 那么久插入
    if (updataStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    updataStmt.close()
    insertStmt.close()
    conn.close()
  }
}
