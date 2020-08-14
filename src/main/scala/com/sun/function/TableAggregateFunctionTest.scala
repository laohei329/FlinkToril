package com.sun.function

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @author ：sun
 * @date ：Created in 2020/8/14 9:25
 * @description： TODO
 * @version: 1.0
 */
object TableAggregateFunctionTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val settings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        // 读取数据
        val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
        val inputStream = env.readTextFile(path)
        //    val inputStream = env.socketTextStream("localhost", 7777)

        // 先转换成样例类类型（简单转换操作）
        val dataStream = inputStream
            .map(data => {
                val arr = data.split(",")
                SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SourceReading](Time.seconds(1)) {
                override def extractTimestamp(element: SourceReading): Long = element.timestamp * 1000L
            })

        val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)
        val top2Agg = new TableAggFunction
        val top2Table: Table = sensorTable.groupBy('id).flatAggregate(top2Agg('temperature) as('temp, 'count))
            .select('id, 'temp, 'count)
        top2Table.toRetractStream[Row].print("table2")
        env.execute("tableTest")
    }

}

class Top2TempAcc {
    var highestTemp: Double = Double.MinValue

    var secoundHighestTemp: Double = Double.MinValue
}

//自定义表聚合函数  提取温度最高的两个
class TableAggFunction extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
        if (temp > acc.highestTemp) {
            acc.secoundHighestTemp = acc.highestTemp
            acc.highestTemp = temp
        }else if(temp>acc.secoundHighestTemp){
            acc.secoundHighestTemp=temp
        }
    }
    //实现以个输出的方法 处理完所有数据后输出 方法名不能变
    def emitValue(acc:Top2TempAcc,out:Collector[(Double,Int)]): Unit ={
        out.collect((acc.highestTemp,1))
        out.collect((acc.secoundHighestTemp,2))
    }



}
