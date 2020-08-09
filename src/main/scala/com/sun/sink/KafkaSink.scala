package com.sun.sink

import java.util.Properties

import com.sun.apitest.SourceReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @author ：sun
 * @date ：Created in 2020/8/7 19:11
 * @description： TODO
 * @version: 1.0
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //kafkaSink 消费者
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092")
    properties.setProperty("group.id", "consumer-group")
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("kafkaSink", new SimpleStringSchema(), properties))
    val kafkaConsumer: DataStream[SourceReading] = kafkaDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    kafkaConsumer.print("kafka consumer")
    val kafkaStrDS: DataStream[String] = kafkaConsumer.map(data=>data.toString)
    //kafkaSink 生产者
    kafkaStrDS.addSink(new FlinkKafkaProducer011[String]("hadoop202:9092","kafkaProducer",new SimpleStringSchema()))
    env.execute("kafka test")
  }

}
