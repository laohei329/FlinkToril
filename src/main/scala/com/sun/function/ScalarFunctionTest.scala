package com.sun.function

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.types.Row

/**
 * @author ：sun
 * @date ：Created in 2020/8/13 21:29
 * @description： TODO
 * @version: 1.0
 */
object ScalarFunctionTest {
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
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(1)) {
      override def extractTimestamp(element: SourceReading) = element.timestamp * 1000L
    })
    val sourceTable: Table = tableEnv.fromDataStream(sourceDS)


    //hashCode
    val hashcode = new HashCode(12)
    sourceTable.select(
      'id, hashcode('id)
    ).toAppendStream[Row].print()
    val split = new Split("_")
    sourceTable.joinLateral(split('id) as('word, 'length))
      .select('id, 'word, 'length).toAppendStream[Row].print("split")
    env.execute("scalar function test")
  }
}

class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String) = {
    s.hashCode * factor
  }
}

class Split(separator: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    str.split(separator).foreach(
      word => collect((word, word.length))
    )
  }
}