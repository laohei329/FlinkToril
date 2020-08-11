package com.sun.table_SQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._


/**
 * @author ：sun
 * @date ：Created in 2020/8/10 21:59
 * @description： TODO
 * @version: 1.0
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /* //1.1 基于老版本planner的流处理
     val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
       .useOldPlanner() //将旧的Flink planner设置为所需的模块。
       .inStreamingMode()
       .build()
     val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
     // 1.2 基于老版本的批处理
     val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
     val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

     //1.3 基于blink planner 的流处理
     val blinkSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
       .useBlinkPlanner()
       .inStreamingMode() //设置组件应该在流模式下工作
       .build()
     val blinkTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkSettings)
     //1.4 基于blinkplanner的批处理
     val blinkBatchSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
       .useBlinkPlanner()
       .inBatchMode()
       .build()
     val blinkBatchEnv: TableEnvironment = TableEnvironment.create(blinkBatchSettings)
 */
    val path = "E:\\FinkWordSp\\FlinkToril\\src\\main\\resources\\sensor.txt"
    //2.链接外部系统  读取数据  注册表

    //2.1 从文件中读取数据
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.connect(new FileSystem().path(path))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.DOUBLE())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable") //在给定路径中注册底层属性描述的表。


    //2.2 从kafka读取数据
    // val properties = new Properties()
    // properties.setProperty("zookeeper.connect","hadoop202:2181")
    // properties.setProperty("bootstrap.servers","hadoop202:9092")
    //tableEnv.connect(new Kafka().properties(properties).topic("sensor").version("0.11"))
    tableEnv.connect(new Kafka()
      .topic("sensor")
      .version("0.11")
      .property("zookeeper.connect", "hadoop202:2181")
      .property("bootstrap.servers", "hadoop202:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")  //创建一张临时表


    //3. 查询转换
    //3.1 使用table api
    //path 要扫描的表API对象的路径。  之前设置的有
    //val sensorTable: Table = tableEnv.from("inputTable")  //根据之前设置的路径取出数据
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val resultTable: Table = sensorTable.select("id,temperature").filter("id='sensor_1'")
    resultTable.toAppendStream[(String, Double)].print("inputTableResult")

    //3.2 直接使用sql查询
    val resultSqlTable: Table = tableEnv.sqlQuery("select id,temperature from kafkaInputTable where id = 'sensor_1'")
    resultSqlTable.toAppendStream[(String,Double)].print("resultSqlTable")

    env.execute("tableapi test")
  }

}
