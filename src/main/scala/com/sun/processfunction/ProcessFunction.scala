package com.sun.processfunction

import com.sun.apitest.SourceReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/10 11:01
 * @description： TODO
 * @version: 1.0
 */
object ProcessFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inoutDS: DataStream[String] = env.socketTextStream("hadoop202", 7777)
    // val latedata = new OutputTag[SourceReading]("latedata")
    val dataStream = inoutDS
      .map(data => {
        val arr = data.split(",")
        SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    //dataStream.process(ProcessFunction)  需要的是ProcessFunction
    //dataStream.keyBy(_.id).process(KeyedProcessFunction)  keyby分组后需要KeyedProcessFunction
    val warnStream: DataStream[String] = dataStream.keyBy(_.id).process(new TemProcessFuntion(10000L))
    warnStream.print("processfunction test")
    env.execute("process function")
  }

}

//连续10个升温的数据
class TemProcessFuntion(interval: Long) extends KeyedProcessFunction[String, SourceReading, String] {
  //最新的时间戳
  lazy val lastTmpState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  //定时器的时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  //处理输入流中的一个元素。 每一个元素都会调用这个方法
  override def processElement(value: SourceReading, ctx: KeyedProcessFunction[String, SourceReading, String]#Context, out: Collector[String]): Unit = {

    val lastTm: Double = lastTmpState.value()
    val timerTs: Long = timerTsState.value()
    val newTm: Double = value.temperature
    //更新温度
    lastTmpState.update(newTm)
    //当前温度与上次温度的差
    val diff = newTm - lastTm
    //// 如果温度上升，且没有定时器，那么注册当前时间10s之后的定时器
    if (diff > 0 && timerTs == 0L) {
      //得到当前的时间并且加上10s
      val ts: Long = ctx.timerService().currentProcessingTime() + interval
      //注册一个计时器，当处理时间超过给定时间时触发。即10后出发定时器
      ctx.timerService().registerProcessingTimeTimer(ts)
      //更新定时器
      timerTsState.update(ts)
    } else if (diff < 0) {
      //温度下降 不符合连续升温的条件 删除定时器
      ctx.timerService().deleteProcessingTimeTimer(timerTs)
      timerTsState.clear()
    }
    //其他的情况 diff>=0 timerTs!=0 的时候直接更新最新的时间戳

  }

  //当触发器被出发后调用这个方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SourceReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval / 1000 + "秒连续上升")
    timerTsState.clear()
  }

}

class MyKeyedProcessFunction(interval: Long) extends KeyedProcessFunction[String, SourceReading, String] {
  lazy val mystate: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("mystate", classOf[String]))

  //每一条数据都会执行一次
  override def processElement(value: SourceReading,
                              ctx: KeyedProcessFunction[String, SourceReading, String]#Context,
                              out: Collector[String]): Unit = {
    ctx.getCurrentKey //获取处理元素的key值
    ctx.timerService().currentWatermark() //获取当前的水位线
    ctx.timerService().currentProcessingTime() //获取当前处理的时间
    ctx.timestamp() //当前正在处理的元素的时间戳或触发计时器的时间戳。
    //注册一个计时器，当事件时间水印经过给定时间时触发
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L)
  }


}