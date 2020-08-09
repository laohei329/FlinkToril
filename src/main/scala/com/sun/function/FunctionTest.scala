package com.sun.function

import com.sun.apitest.SourceReading
import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/7 14:37
 * @description： TODO
 * @version: 1.0
 */
object FunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val readDS: DataStream[String] = env.readTextFile("E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt")
    val filterDS: DataStream[SourceReading] = readDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    ).keyBy("id").filter(new MyFilterFunction())
    //filterDS.print("myFilterFunction")

    val MYfilterDS: DataStream[SourceReading] = readDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    ).keyBy("id").filter(new RichFilterFunction[SourceReading] {
      override def filter(value: SourceReading) = value.temperature > 30
    })
    //MYfilterDS.print("filter test")
    MYfilterDS.filter(_.temperature>35).print("lambda")

    env.execute("function test")
  }

}
class MyFilterFunction() extends FilterFunction[SourceReading]{
  override def filter(value: SourceReading): Boolean = {
    value.temperature>30
  }
}
