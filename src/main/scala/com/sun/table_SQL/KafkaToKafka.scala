package com.sun.table_SQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @author ：sun
 * @date ：Created in 2020/8/11 9:48
 * @description： TODO
 * @version: 1.0
 */
object KafkaToKafka {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //从文件中读取数去
    /*    tableEnv.connect(new FileSystem().path("E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"))
          .withFormat(new Csv())
          .withSchema(new Schema()
            .field("id", DataTypes.STRING())
            .field("temp", DataTypes.DOUBLE())
            .field("temp", DataTypes.DOUBLE())
          ).createTemporaryTable("fileInput")
        */
    tableEnv.connect(new Kafka()
      .topic("sensor")
      .version("0.11")
      .property("zookeeper.connect", "hadoop202:2181")
      .property("bootstrap.servers", "hadoop202:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema() //设置对应的字段对应的名字
        .field("id", DataTypes.STRING())
        .field("time", DataTypes.DOUBLE())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputStream")

    tableEnv.connect(new Kafka().topic("tableout").version("0.11")
      .property("zookeeper.connect", "hadoop202:2181")
      .property("bootstrap.servers", "hadoop202:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutPutStream")

    val kafkaInputTable: Table = tableEnv.from("kafkaInputStream")
    //tab.select('key, 'value.avg + " The average" as 'average)
    val sqlResult: Table = kafkaInputTable.select("id,temp").filter("id = 'sensor_1'")
    sqlResult.toAppendStream[(String, Double)].print("kafkaInputStream")

    //讲查询结果输入到kafka
    sqlResult.insertInto("kafkaOutPutStream")

    env.execute("file out put")
  }

}
