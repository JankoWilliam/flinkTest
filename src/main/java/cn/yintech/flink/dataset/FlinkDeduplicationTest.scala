package cn.yintech.flink.dataset

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object FlinkDeduplicationTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("E:\\work\\004_idea_workspace\\flinkTest\\src\\main\\resources\\words.txt")
    val value = text.flatMap(_.split(" ")).map((_,1))

    val value1 = value.keyBy(0)

    value1.print()



    env.execute("FlinkDeduplicationTest")



  }
}
