package com.sun.outsidestream

import com.sun.apitest.SourceReading
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/10 15:04
 * @description： TODO
 * @version: 1.0
 */
object OutSlideTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //env.setStateBackend(new FsStateBackend(" "))
    //env.setStateBackend(new MemoryStateBackend())
    val inoutDS: DataStream[String] = env.socketTextStream("hadoop202", 7777)
    // val latedata = new OutputTag[SourceReading]("latedata")
    val dataStream = inoutDS
      .map(data => {
        val arr = data.split(",")
        SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    val highTmpDS: DataStream[SourceReading] = dataStream.keyBy(_.id).process(new SplitTmpFunction(30.0))
    highTmpDS.print("high")
    highTmpDS.getSideOutput(new OutputTag[SourceReading]("slide out")).print("low")

    env.execute("outslide")
  }

}

class SplitTmpFunction(d: Double) extends ProcessFunction[SourceReading, SourceReading] {
  override def processElement(value: SourceReading, ctx: ProcessFunction[SourceReading, SourceReading]#Context, out: Collector[SourceReading]): Unit = {
    if (value.temperature > d) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[SourceReading]("slide out"), value)
    }
  }
}
