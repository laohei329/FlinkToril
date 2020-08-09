package com.sun.sink

import com.sun.apitest.SourceReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._



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
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    val fileDS: DataStream[String] = env.readTextFile(path)
    val sourceDS: DataStream[SourceReading] = fileDS.map(data => {
      val strs: Array[String] = data.split(",")
      SourceReading(strs(0), strs(1).toLong, strs(2).toDouble)
    }
    )
  //  sourceDS.writeAsCsv("E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\out.txt")
    val outpath="E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\out2.txt"
    sourceDS.addSink(StreamingFileSink.forRowFormat(new Path(outpath),new SimpleStringEncoder[SourceReading]()).build())


    env.execute("file sink")
  }

}
