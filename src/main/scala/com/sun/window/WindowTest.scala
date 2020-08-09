package com.sun.window

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author ：sun
 * @date ：Created in 2020/8/8 0:49
 * @description： TODO
 * @version: 1.0
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inoutDS: DataStream[String] = env.socketTextStream("hadoop202", 7777)
    val dataStream = inoutDS
      .map(data => {
        val arr = data.split(",")
        SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })   // // 升序数据提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(1)) {
      override def extractTimestamp(element: SourceReading) = {
        element.timestamp * 1000L
      }
    })
    dataStream
      .map(data=>(data.id,data.temperature,data.timestamp))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))//滚动时间窗口


    env.execute("window test")
  }

}
