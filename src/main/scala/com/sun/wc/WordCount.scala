package com.sun.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputPath = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\input.txt"
    val fileDS: DataSet[String] = env.readTextFile(inputPath)
    val wordcountSD: DataSet[(String, Int)] = fileDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    wordcountSD.print()
  }
}
