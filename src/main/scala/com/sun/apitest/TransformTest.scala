package com.sun.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/5 20:40
 * @description： TODO
 * @version: 1.0
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputpath = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileStreamDS: DataStream[String] = env.readTextFile(inputpath)
    //    fileStreamDS.print()
    val sourceDS: DataStream[SourceReading] = fileStreamDS.map { data => {

      val arr: Array[String] = data.split(",")
      SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }
    }
    // 2.分组聚合，输出每个传感器当前最小值
    //    val minDS: DataStream[SourceReading] = sourceDS.keyBy(0).minBy(2)
    val value: DataStream[SourceReading] = sourceDS.keyBy(data => data.id).maxBy("temperature")
    // value.print()
    //    minDS.print()
    //3.过滤出温度大于20的数据
    val filterDS: DataStream[SourceReading] = sourceDS.filter(data => data.temperature > 20)
    //filterDS.print()


    // // 4.需要输出当前最小的温度值，以及最近的时间戳，要用reduce
    val minDS: DataStream[SourceReading] = sourceDS.keyBy("id")
      .reduce((curState, newState) => {
        SourceReading(curState.id, newState.timestamp.max(newState.timestamp), curState.temperature.min(newState.temperature))
      })
    // minDS.print()
    val myreduceDS: DataStream[SourceReading] = sourceDS.keyBy("id").reduce(new MyreduceFunction())
    //myreduceDS.print()
    val splitDS: SplitStream[SourceReading] = sourceDS.split(data =>
      if (data.temperature > 30) Seq("high") else Seq("low")
    )
    val highDS: DataStream[SourceReading] = splitDS.select("high")
   // highDS.print("high")
    val lowDS: DataStream[SourceReading] = splitDS.select("low")
    //lowDS.print("low")
    val addDS: DataStream[SourceReading] = splitDS.select("all")
    val warnData: DataStream[(String, Double)] = highDS.map(data=>(data.id,data.temperature))
    val connectStream: ConnectedStreams[(String, Double), SourceReading] = warnData.connect(lowDS)
    val coMapDS: DataStream[Any] = connectStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    //coMapDS.print()
    val unionDS: DataStream[SourceReading] = highDS.union(lowDS,addDS)
    unionDS.print("union")

    env.execute("API test")
  }

}

class MyreduceFunction() extends ReduceFunction[SourceReading] {
  override def reduce(source1: SourceReading, source2: SourceReading): SourceReading = {
    SourceReading(source1.id, source1.timestamp.max(source2.timestamp), source1.temperature.min(source2.temperature))
  }
}

