package com.sun.sink

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._



/**
 * @author ：sun
 * @date ：Created in 2020/8/7 20:48
 * @description： TODO
 * @version: 1.0
 */
object FileSink {
  def main(args: Array[String]): Unit = {
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
    //样例类的属性名 当做表中的列名  不能不同  如果想要改名字可以在属性中指明并起别名

    //val fileTable: Table = tableEnv.fromDataStream(sourceDS)  这里属性名作为表中的列名
    val fileTable: Table = tableEnv.fromDataStream(sourceDS,'id,'timestamp as('time),'temperature as('temp))

    val sqlResult: Table = fileTable.select("id,temp" ).filter("id = 'sensor_1'")
    sqlResult.toAppendStream[(String,Double)].print()
    //sqlResult.printSchema()//打印变得结构

  //  sourceDS.writeAsCsv("E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\out.txt")
   // val outpath="E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\out2.txt"
   // sourceDS.addSink(StreamingFileSink.forRowFormat(new Path(outpath),new SimpleStringEncoder[SourceReading]()).build())


    env.execute("file sink")
  }

}
