package com.sun.window

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
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
    env.setParallelism(1)
    val inoutDS: DataStream[String] = env.socketTextStream("hadoop202", 7777)
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
    // dataStream.print("data")
    //    val resultDS: DataStream[(String, Double, Long)] = dataStream
    //      .map(data => (data.id, data.temperature, data.timestamp))
    //      .keyBy(_._1)
    //      .window(TumblingEventTimeWindows.of(Time.seconds(15))) //滚动时间窗口(参数  窗口大小  偏移量)
    //      .reduce((curState, newState) => (newState._1, curState._2.max(newState._2), newState._3))
    //    resultDS.print("tumb window")

    val lateTag = new OutputTag[(String, Double, Long)]("latedata")
    val resultDS: DataStream[(String, Double, Long)] = dataStream
      .map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))  //按时间生成窗口  没过15秒申城窗口
      .allowedLateness(Time.minutes(1)) //允许迟到一分钟的数据 在一分钟后才关闭窗口
      .sideOutputLateData(lateTag)

      //.window(TumblingEventTimeWindows.of(Time.seconds(15)))  //滚动窗口函数
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))  //滑动窗口函数
      //.window(EventTimeSessionWindows.withGap(Time.seconds(5))) //会话超时5s后开始计算
      //.countWindow(5)  //滚动计算窗口 按照条数生成窗口

      .reduce((curState, newState) => (newState._1, curState._2.max(newState._2), newState._3))
    resultDS.print("even window")
    resultDS.getSideOutput(lateTag)
    env.execute("window test")
  }

}
