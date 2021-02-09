package cn.yintech.flink.dataStream

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector



object ProcessFunctionTest {
  //传感器读书样列类
  case class SensorReading(id :String,timestamp:Long,temperature:Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("192.168.19.123", 7777)

    val dataStream = stream.map(data=>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    val processedStream = dataStream.keyBy(_.id)
      .process(new TemIncreAlert())

    dataStream.print("input data")
    processedStream.print("processed data")

    env.execute()

  }
  class TemIncreAlert() extends KeyedProcessFunction[String,SensorReading,String]{
    //定义一个状态，用来保存上一个数据的温度值
    lazy val lastTemp:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",
      classOf[Double]))
    //定义一个状态，用来保存定时器的时间戳
    lazy val currentTimer:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("CurrentTimer",
      classOf[Long]))

    override def processElement(i: SensorReading, context: KeyedProcessFunction
      [String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
      //先取出上一个温度值
      val preTemp = lastTemp.value()
      //更新温度值
      lastTemp.update(i.temperature)

      val curTimerTs = currentTimer.value()

      //温度上升且没有设过定时器，则注册定时器
      if (i.temperature>preTemp && curTimerTs == 0){
        val timeTs = context.timerService().currentProcessingTime() + 10000L
        //要获取当前时间  如果你写1s 的话就是1970年00:00:01
        context.timerService().registerProcessingTimeTimer(timeTs)
        currentTimer.update(timeTs)
      }else if(preTemp > i.temperature || preTemp == 0.0){
        //如果温度下降，或是第一条数据，删除定时器并清空状态
        context.timerService().deleteProcessingTimeTimer(curTimerTs)
        currentTimer.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]
      #OnTimerContext, out: Collector[String]): Unit = {
      //输出报警信息
      out.collect(ctx.getCurrentKey + "温度连续上升")
      currentTimer.clear()
    }
  }
}
