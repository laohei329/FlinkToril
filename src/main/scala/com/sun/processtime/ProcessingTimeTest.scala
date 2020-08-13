package com.sun.processtime

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @author ：sun
 * @date ：Created in 2020/8/13 13:51
 * @description： TODO
 * @version: 1.0
 */
object ProcessingTimeTest {
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
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(1)) {
      override def extractTimestamp(element: SourceReading) = element.timestamp*1000L
    })


    //方法一
    val resultTable: Table = tableEnv.fromDataStream(sourceDS, 'id, 'timestamp as ('ts), 'temperature, 'pt.proctime)
      .select('id, 'ts, 'temperature, 'pt)
    //resultTable.toAppendStream[Row].print("processTest")
    //方法二  定义schema的时候制定
    tableEnv.connect(
      new FileSystem().path(path)
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.BIGINT())
        .field("temp" , DataTypes.DOUBLE())
        .field("pt", DataTypes.TIMESTAMP(3)).proctime()
      ).createTemporaryTable("inPutTable")
    //todo 这里添加的
    val resultTable3: Table = tableEnv.from("inPutTable")
    resultTable3.printSchema()
   //resultTable3.toAppendStream[Row].print()

    //ddl中定义
    env.execute("process time")
  }

}
