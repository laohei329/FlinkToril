package com.sun.table_SQL

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @author ：sun
 * @date ：Created in 2020/8/11 21:22
 * @description： TODO
 * @version: 1.0
 */
object OutPutMySql {
  def main(args: Array[String]): Unit = {
    //1.环境的准备
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    )

    //讲DataStream  转换成table          pt指明是时间语义字段
    val fileTable: Table = tableEnv.fromDataStream(sourceDS,'id,'timestamp ,'temperature ,'pt.proctime)
    val resultTable: Table = fileTable.select('id, 'timestamp as ('ts), 'temperature as ('temp),'pt)
    resultTable.toAppendStream[Row].print("resultTable")

    //resultTable 输入到jdbc

    //jdbcOutPut 是运行时环境的虚拟的表  sensor_table为MySQL中实际存在的表
    //todo 这个地方起名一定要注意
    val sinkDDl =
    """
      |create table jdbcOutPut(
      |id String,
      |ts bigint,
      |temp DOUBLE,
      |pt bigint
      |) with(
      |'connector.type' = 'jdbc',
      |'connector.url' = 'jdbc:mysql://hadoop202:3306/test',
      |'connector.table' = 'sensor_table',
      |'connector.username'='root',
      |'connector.password'='000000'
      |)
      |""".stripMargin
    //    tableEnv.sqlUpdate(sinkDDl)
    //    resultTable.insertInto("jdbcOutPut")


    val aggTable: Table = fileTable.groupBy('id).select('id, 'id.count as ('num))
    //aggTable.toRetractStream[(String, Long)].print("aggTable")
    val sinkCountDDl =
      """
        |create table jdbcOutPutCount(
        |id String,
        |num bigint
        |) with(
        |'connector.type' = 'jdbc',
        |'connector.url' = 'jdbc:mysql://hadoop202:3306/test',
        |'connector.table' = 'sensor_count',
        |'connector.username'='root',
        |'connector.password'='000000'
        |)
        |""".stripMargin
   // tableEnv.sqlUpdate(sinkCountDDl)
   // aggTable.insertInto("jdbcOutPutCount")

    //todo 查看执行计划
    //val explainStr: String = tableEnv.explain(aggTable)
   // println(explainStr)
    //aggTable.toRetractStream[(String,Long)].var.print()
    // aggTable.toRetractStream[Row].print()

    //
    env.execute("jdbc test")
  }

}
