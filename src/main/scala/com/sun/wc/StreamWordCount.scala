package com.sun.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: String = params.get("port")
    val inputDS: DataStream[String] = env.socketTextStream(host, port.toInt)
    val wordCountDS: DataStream[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    wordCountDS.print()
    env.execute("wordcount")
  }
}
