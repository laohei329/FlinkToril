package com.sun.window

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @author ：sun
 * @date ：Created in 2020/8/13 17:14
 * @description： TODO
 * @version: 1.0
 */
object GroupWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(1)) {
      override def extractTimestamp(element: SourceReading) = element.timestamp * 1000L
    })
    //todo 想要使用groupwindow窗口表需要注册水印 和制定时间戳


    val table: Table = tableEnv.fromDataStream(sourceDS, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    //1.1table api
    val aggTable: Table = table
      .window(Tumble over 10.seconds on 'ts as 'tw) //滚动时间窗口宽度是10s  必须制定别名
      .groupBy('id, 'tw) //按照id进行分组
      .select('id, 'id.count, 'temperature.avg, 'tw.end) //对表进行聚合操作
    // aggTable.toRetractStream[Row].print("groupwindowtest")


    //1.2sql
    tableEnv.createTemporaryView("sensor", table)
    val sql =
      """
        |select id,count(id),avg(temperature),tumble_end(ts,interval '10' second)
        |from sensor
        |group by
        |id,tumble(ts,interval '10' second)
        |""".stripMargin
    val resultTable: Table = tableEnv.sqlQuery(sql)
 //   resultTable.toRetractStream[Row].print("sqltable")


    //2.over window

    //2.1 table api
    // preceding 指向前追溯  following 指向后追溯到多少
    val overtable: Table = table.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)
    //overtable.toRetractStream[Row].print("over")

    val overSqlResult: Table = tableEnv.sqlQuery(
      """
        |select
        |  id,
        |  ts,
        |  count(id) over ow,
        |  avg(temperature) over ow
        |from sensor
        |window ow as (
        |  partition by id
        |  order by ts
        |  rows between 2 preceding and current row
        |)
        |""".stripMargin)
    overSqlResult.toRetractStream[Row].print()
    env.execute("groupwindow test")
  }
}
