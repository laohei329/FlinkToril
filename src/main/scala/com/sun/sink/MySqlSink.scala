package com.sun.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sun.apitest.SourceReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/9 10:36
 * @description： TODO
 * @version: 1.0
 */
object MySqlSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceDS: DataStream[String] = env.readTextFile("E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt")
    val dataDS: DataStream[SourceReading] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }
    )
    dataDS.addSink(new MyJdbcSink())
    env.execute("mysql test")
  }

}

class MyJdbcSink() extends RichSinkFunction[SourceReading] {
  var conn: Connection = _
  var insertStm: PreparedStatement = _
  var updataStm: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop202:3306/test", "root", "000000")
    insertStm = conn.prepareStatement("insert into sensor_temp (id, temperature) values (?, ?)")
    updataStm = conn.prepareStatement("update sensor_temp set temperature=? where id=?")
  }

  override def invoke(value: SourceReading, context: SinkFunction.Context[_]): Unit = {
    // 先执行更新操作，查到就更新
    updataStm.setDouble(1, value.temperature)
    updataStm.setString(2, value.id)
    updataStm.execute()
    //如果没有查询到数据 就进行插入操作
    if (updataStm.getUpdateCount == 0) {
      insertStm.setString(1, value.id)
      insertStm.setDouble(2, value.temperature)
      insertStm.execute()
    }


  }

  override def close(): Unit = {
    updataStm.close()
    insertStm.close()
    conn.close()
  }

}