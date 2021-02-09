package cn.yintech.flink.source

import cn.yintech.flink.source.FlinkBean.SensorReading
import org.apache.flink.streaming.api.scala
import org.apache.flink.streaming.api.scala._


object MySQLSourceSinkApp {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //调用addSource以此来作为数据输入端
    val stream: scala.DataStream[SensorReading] = env.addSource(new MySQLSource)


    //调用addSink以此来作为数据输出端
    stream.addSink(new MySQLSink())

    // 打印流
    stream.print()

    // 执行主程序
    env.execute()
  }

}