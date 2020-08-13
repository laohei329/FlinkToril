package com.sun.table_SQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors._

/**
 * @author ：sun
 * @date ：Created in 2020/8/11 19:47
 * @description： TODO
 * @version: 1.0
 */
object OutPutES {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(path))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("temp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")
    val fileTable: Table = tableEnv.from("inputTable")

    val resultTable: Table = fileTable.select('id, 'temp)
      //.filter('id === "sensor_1")

    val aggTable: Table = resultTable
      .groupBy("id")
      .select('id, 'id.count as('count) )


    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("hadoop202", 9200, "http")
      .index("sensortest")
      .documentType("temperature")
    )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT()))
      .createTemporaryTable("esOutput")

    aggTable.insertInto("esOutput")


    env.execute("output es test")
  }

}
