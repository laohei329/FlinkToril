package com.sun.chickpoint

import com.sun.apitest.SourceReading
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._

/**
 * @author ：sun
 * @date ：Created in 2020/8/10 18:03
 * @description： TODO
 * @version: 1.0
 */
object ChickPointTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStateBackend(new FsStateBackend(""))
    //env.setStateBackend()
    //状态检查点之间的时间间隔(毫秒)。
    env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE)

   //checkpoint 配置
    val config: CheckpointConfig = env.getCheckpointConfig
    config.setCheckpointTimeout(1000L) //设置timeout时间
    config.getCheckpointTimeout
    config.setMaxConcurrentCheckpoints(2) //设置最大并发
    config.setMinPauseBetweenCheckpoints(500L) //checkpoint之间头尾的间隔
    config.setPreferCheckpointForRecovery(true) //设置当存在更近期的保存点时，作业恢复是否应回退到检查点。
   //重启策略配置
    //env.setRestartStrategy(RestartStrategies)

    val inoutDS: DataStream[String] = env.socketTextStream("hadoop202", 7777)
    // val latedata = new OutputTag[SourceReading]("latedata")
    val dataStream = inoutDS
      .map(data => {
        val arr = data.split(",")
        SourceReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    env.execute("chick point")
  }

}
