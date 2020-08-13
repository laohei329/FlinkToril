package com.sun.processtime

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}

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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    //方法一  ds转换时间表
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    )
      //为数据流中的元素分配时间戳，并定期创建水印来表示事件时间进程。   并指明时间戳间隔为1s
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(1)) {
      override def extractTimestamp(element: SourceReading) = element.timestamp
    })
    //event time 使用的是事件中自带的的时间戳作为时间戳
    //直接指明自带的字段为eventtime
    //tableEnv.fromDataStream(sourceDS,'id,'timestamp.rowtime,'temperature)
    //TODO 追加时间字段  如果上面没有注册时间戳此处添加字段会报错  eventtime 字段必须是事件中的字段
    val resultTable: Table = tableEnv.fromDataStream(sourceDS,'id,'timestamp,'temperature,'timestamp.rowtime as 'ts)
    //resultTable.toAppendStream[Row].print()
    // 方法二 在tablechema中指明

    tableEnv.connect(new FileSystem().path(path))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .rowtime(new Rowtime()
            .timestampsFromField("timestamp")//// 从字段中提取时间戳
            .watermarksPeriodicBounded(1000)  //// watermark延迟1秒
        )
        .field("temperature", DataTypes.DOUBLE())
      )
        .createTemporaryTable("evenTimeInput")

    val resultTable2: Table = tableEnv.from("evenTimeInput")
    resultTable2.printSchema()


    env.execute("event time")
  }
}
