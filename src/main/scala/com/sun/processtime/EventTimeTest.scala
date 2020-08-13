package com.sun.processtime

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/13 15:21
 * @description： TODO
 * @version: 1.0
 */
object EventTimeTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    )
    //event time 使用的是事件中自带的的时间戳作为时间戳
    tableEnv.fromDataStream(sourceDS,'id,'timestamp.rowtime,'temperature)

    env.execute("event time")
  }
}
