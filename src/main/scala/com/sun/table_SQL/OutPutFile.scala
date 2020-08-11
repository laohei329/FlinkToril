package com.sun.table_SQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}


/**
 * @author ：sun
 * @date ：Created in 2020/8/11 15:49
 * @description： TODO
 * @version: 1.0
 */
object OutPutFile {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
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
    val kafkaTable: Table = tableEnv.from("kafkaInputStream")
    val resultTable: Table = kafkaTable.select('id,'temp).filter('id==="sensor_1")
    resultTable.toAppendStream[(String,Double)].print("result")


    tableEnv.connect(new FileSystem().path("E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\out2.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.DOUBLE())
      ).createTemporaryTable("output")
    //resultTable.insertInto("output")
    //聚合操作
    val aggTable: Table = kafkaTable
      .groupBy("id")
      .select('id, 'id.count as('count) )
    aggTable.toRetractStream[(String,Long)].print("agg")
    tableEnv.connect(new FileSystem().path("E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\out3.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("count",DataTypes.BIGINT())
      ).createTemporaryTable("output2")
    aggTable.insertInto("output2")
    env.execute("outtofile")
  }
}
