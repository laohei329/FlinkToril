package com.sun.sink

import com.sun.apitest.SourceReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author ：sun
 * @date ：Created in 2020/8/8 0:30
 * @description： TODO
 * @version: 1.0
 */
object MyRedisSink {
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
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop202")
      .setPort(6379)
      .build()


    sourceDS.addSink(new RedisSink[SourceReading](config,new MyRedisMapper))


    env.execute("Redis Sink")
  }

}

class MyRedisMapper() extends RedisMapper[SourceReading] {
  override def getCommandDescription: RedisCommandDescription = {
    // 定义保存数据写入redis的命令，HSET 表名 key value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }


  override def getKeyFromData(data: SourceReading): String = {
    data.id
  }
  // 将温度值指定为valu
  override def getValueFromData(data: SourceReading): String = {

    data.temperature.toString
  }
}

