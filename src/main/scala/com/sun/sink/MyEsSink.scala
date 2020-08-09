package com.sun.sink

import java.util

import com.sun.apitest.SourceReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Request, Requests}

/**
 * @author ：sun
 * @date ：Created in 2020/8/7 21:07
 * @description： TODO
 * @version: 1.0
 */
object MyEsSink {
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
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop202", 9200))


    sourceDS.addSink(new ElasticsearchSink.Builder[SourceReading](
      httpHosts, new MyEsSinkFunction
    ).build())
    env.execute("es sink")
  }
}

class MyEsSinkFunction extends ElasticsearchSinkFunction[SourceReading] {
  override def process(t: SourceReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    // 包装一个Map作为data source
    val dataSource = new util.HashMap[String, String]()
    dataSource.put("id", t.id)
    dataSource.put("temperature", t.temperature.toString)
    dataSource.put("timestamp", t.timestamp.toString)
    // 创建index request，用于发送http请求
    val indexRequest: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("readingdata")
      .source(dataSource)

    requestIndexer.add(indexRequest)
  }
}
