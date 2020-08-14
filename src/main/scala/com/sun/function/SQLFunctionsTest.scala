package com.sun.function

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/13 21:20
 * @description： TODO
 * @version: 1.0
 */
object SQLFunctionsTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    )
  val sourceTable: Table = tableEnv.fromDataStream(sourceDS)
    sourceTable.select('id,'timestamp as('times),'temperature as 'temp)
        .filter('id==="sensor_1")

    env.execute("sql function test")
  }
}

