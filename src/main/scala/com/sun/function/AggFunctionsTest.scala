package com.sun.function

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * @author ：sun
 * @date ：Created in 2020/8/14 8:39
 * @description： TODO
 * @version: 1.0
 */
object AggFunctionsTest {
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
        val sourceTable: Table = tableEnv.fromDataStream(sourceDS)
        val temp = new AvgTemp
        val resultTable: Table = sourceTable.groupBy('id).aggregate(temp('temperature) as 'avgtemp).select('id, 'avgtemp)
       // resultTable.toRetractStream[Row].print("tableResult")


      tableEnv.createTemporaryView("sourceTable",sourceTable)
      tableEnv.registerFunction("avgTemp",temp)
      val sqlTable: Table = tableEnv.sqlQuery(
        """
          |select id, avgTemp(temperature)
          |from
          |sourceTable
          |group by id
          |""".stripMargin)
      sqlTable.toRetractStream[Row].print("sql")

        env.execute("agg test")
    }

}

//泛型第一个参数为  聚合结果的类型
//泛型
class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
}

class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {

    //accumulator
    override def getValue(accumulator: AvgTempAcc): Double = {
        accumulator.sum / accumulator.count
    }

    //初始化
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    // todo 还要实现一个具体的处理计算函数，accumulate  定义好的名字不能改变
    def accumulate(accumulator: AvgTempAcc, temp: Double): Unit = {
        accumulator.sum += temp
        accumulator.count += 1
    }
}