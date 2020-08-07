package com.sun.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
 * @author ：sun
 * @date ：Created in 2020/8/5 18:56
 * @description： TODO
 * @version: 1.0
 */
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2) //设置并行度
    val sources = List(
      SourceReading("sensor_1", 1547718199, 35.8),
      SourceReading("sensor_6", 1547718201, 15.4),
      SourceReading("sensor_7", 1547718202, 6.7),
      SourceReading("sensor_10", 1547718205, 38.1)
    )
    //fromCollection  元素最好是可序列化的
    // 1. 从集合中读取数据
    val sourceDS: DataStream[SourceReading] = env.fromCollection(sources)
    //sourceDS.print("source test 1")
    val anyDS: DataStream[Any] = env.fromElements(1, 35, "xxxx")
    //anyDS.print("anyDS")
    //2.从文件中读取数据
    val inputpath = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(inputpath)
    //fileDS.print("fileDS")

    //3.从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092")
    properties.setProperty("group.id", "consumer-group")
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    //kafkaDS.print("kafkaDS")

    //4 自定义source
    val mysourceDS: DataStream[SourceReading] = env.addSource(new MySource())
    mysourceDS.print()

    env.execute("source test")
  }

}

class MySource() extends SourceFunction[SourceReading] {
  //定义一个flag用来表示数据源是否正常
  var running: Boolean = true

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[SourceReading]) = {
    val random = new Random()
    var tuples: IndexedSeq[(String, Double)] = 1.to(10).map(i => ("sensor_" + i, random.nextDouble() * 10))
    while (running) {
      // 在上次数据基础上微调，更新温度值  正态分布
      tuples = tuples.map(data => (data._1, data._2 + random.nextGaussian()))
      val curTime: Long = System.currentTimeMillis()
      tuples.foreach(
        data => ctx.collect(SourceReading(data._1, curTime, data._2))
      )
    }


  }

}

/**
 *
 * @param id          类的id
 * @param timestamp   时间戳
 * @param temperature 温度
 */
case class SourceReading(id: String, timestamp: Long, temperature: Double)
