package com.sun.watermark

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ：sun
 * @date ：Created in 2020/8/9 20:28
 * @description： TODO
 * @version: 1.0
 */
object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inoutDS: DataStream[String] = env.socketTextStream("hadoop202", 7777)
    val latedata = new OutputTag[SourceReading]("latedata")
    val dataStream = inoutDS
      .map(data => {
        val arr = data.split(",")
        SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }) // // 升序数据提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(1)) {
        override def extractTimestamp(element: SourceReading) = {
          element.timestamp * 1000L
        }
      })
    //按照升序尽进行排序过的
    //dataStream.assignAscendingTimestamps(_.timestamp*1000)
//    val assignWaterDS: DataStream[SourceReading] = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(3)) {
//      override def extractTimestamp(element: SourceReading) = {
//        element.timestamp * 1000L
//      }
//    })
    val assignWaterDS: DataStream[SourceReading] = dataStream.assignTimestampsAndWatermarks(new MyAssign)

    val maxDS: DataStream[SourceReading] = assignWaterDS.keyBy("id")
      /*
      //timestamp - (timestamp - offset + windowSize) % windowSize
      设置时间戳的起始位置
       */
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(15))
      .sideOutputLateData(latedata)
      .reduce((curSR, newSR) =>
        SourceReading(curSR.id, newSR.timestamp, curSR.temperature.max(newSR.temperature))
      )
    maxDS.print("watermaker test")
    maxDS.getSideOutput(latedata).print("latedata")


    env.execute("water test")
  }

}

//自定义 assign
class MyAssign() extends AssignerWithPeriodicWatermarks[SourceReading]() {
  val bound: Long = 60 * 1000 //设置延迟时间为一分钟
  var maxTs: Long = Long.MinValue //观察到的最大时间戳


  //返回当前的时间差
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }


  //指定时间戳
  override def extractTimestamp(element: SourceReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}
