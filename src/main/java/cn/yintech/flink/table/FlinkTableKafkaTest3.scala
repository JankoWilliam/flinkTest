package cn.yintech.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

import org.apache.flink.table.api._
import org.apache.flink.api.scala._

object FlinkTableKafkaTest3 {

  def main(args: Array[String]): Unit = {

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)

    val sinkDDL: String =
      """
        |create table dataTable (
        |  id varchar(20) not null,
        |  ts bigint,
        |  temperature double,
        |  pt AS PROCTIME()
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = 'E:\work\004_idea_workspace\flinkTest\src\main\resources\sensor.txt',
        |  'format.type' = 'csv'
        |)
  """.stripMargin

    tableEnv.sqlUpdate(sinkDDL) // 执行 DDL
  }

}
