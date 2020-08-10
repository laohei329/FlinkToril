package com.sun.valuestate

import java.util

import com.sun.apitest.{MyreduceFunction, SourceReading}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/9 22:44
 * @description： TODO
 * @version: 1.0
 */
object ValueStateTest {
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
    //对温度传感器温度值跳变超过10度 报警
    val alertDS: DataStream[(String, Double, Double)] = dataStream.keyBy("id").flatMapWithState[(String, Double, Double), Double] {
      case (data: SourceReading, None) => (List.empty, Some(data.temperature))
      case (data: SourceReading, lastTem: Some[Double]) => {
        val diff: Double = (data.temperature - lastTem.get).abs
        if (diff > 10.0) {
          (List((data.id, lastTem.get, data.temperature)), Some(data.temperature))
        } else {
          (List.empty, Some(data.temperature))
        }
      }
    }
    alertDS.print("alertDS")
    val alterDS: DataStream[(String, Double, Double)] = dataStream.keyBy("id").flatMap(new MyRichMapper(10.0))
    alterDS.print("xxx")
    env.execute("value state")
  }

}

class MyRichMapper(threshold: Double) extends RichFlatMapFunction[SourceReading, (String, Double, Double)] {
  lazy val lastTem: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val flagState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("flag", classOf[Boolean],true))

  override def flatMap(value: SourceReading, out: Collector[(String, Double, Double)]): Unit = {
    val flag: Boolean = flagState.value()
    val diff: Double = (lastTem.value - value.temperature).abs
    if (!flag && diff > threshold) {
      //发出一种记录
      out.collect((value.id, lastTem.value, value.temperature))
    }
    flagState.update(false)
    lastTem.update(value.temperature)
  }
}


class MyRichMapperTest extends RichMapFunction[SourceReading, String] {
  var valueState: ValueState[Double] = _ //状态值
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))
  //MapStateDescriptor(name: String, keyClass: Class[UK], valueClass: Class[UV])
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapstate", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SourceReading] =
    getRuntimeContext.getReducingState(new ReducingStateDescriptor[SourceReading]("reduceState", new MyreduceFunction, classOf[SourceReading]))

  override def map(value: SourceReading): String = {
    //状态的读写
    val myvalue: Double = valueState.value()
    valueState.update(value.temperature)

    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    //状态添加
    listState.addAll(list)
    //状态重新设置
    listState.update(list)


    mapState.contains("sensor_1")
    //通过key获取值
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)

    val map = new util.HashMap[String, Double]()
    mapState.put("sensor_1", 1.3)
    mapState.put("sensor_2", 1.3)
    mapState.putAll(map)

    val reading: SourceReading = reduceState.get()
    reduceState.add(value)
    value.id
  }

  override def open(parameters: Configuration): Unit = {
    //第二个参数 对状态中值的类型进行类型分类。
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
  }
}
