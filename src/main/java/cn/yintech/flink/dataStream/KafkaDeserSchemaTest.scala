package cn.yintech.flink.dataStream

import java.util.Properties

import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.util.Collector


object KafkaDeserSchemaTest {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
    //    env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "LiveVisitCountFlink01")

    val topic = "sc_md"

    val kafkaSource = new FlinkKafkaConsumer011[ObjectNode](topic, new JSONKeyValueDeserializationSchema(true), properties)

    val dataStream: DataStream[ObjectNode] = env.addSource(kafkaSource)
      .setParallelism(3)
      .name("sourceStream")
      .uid("sourceStream01")

    //    dataStream.process(new ProcessFunction[ObjectNode,String] {
    //      override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, String]#Context, out: Collector[String]): Unit = {
    //        val event = value.get("value").get("event").asText()
    //        val _hybrid_h5 = value.get("value").get("_hybrid_h5").asBoolean()
    //        val sex = value.get("value").get("sex").asText()
    //        println("event:"+event+",_hybrid_h5:"+_hybrid_h5+",sex:"+sex)
    //      }
    //    })

    val eventData = dataStream.map(obj => {
      val event = obj.get("value").get("event").asText()
      val age = obj.get("value").get("age").asText()
      val userID = obj.get("value").get("properties").get("userID").asText()
      val offset = obj.get("metadata").get("offset").asText()
      val topic = obj.get("metadata").get("topic").asText()
      val partition = obj.get("metadata").get("partition").asText()
      (event, age, userID, s"消费的主题是：$topic,分区是：$partition,当前偏移量是：$offset")
    })

//    eventData.keyBy(_._1)
//      .timeWindow(Time.seconds(5))
//      .reduce((v1, v2) => {
//        if (v1._2 > v2._2) v2 else v1
//      },
//        (key:(String,String,String,String),
//         context: ProcessWindowFunction[_, _, _, TimeWindow]#Context,
//         minAge : Iterable[(String, String, String, String)],
//         out: Collector[(Long, (String, String, String, String))]) => {
//          val min = minAge.iterator.next()
//          out.collect((context.window().getStart, min))
//        }
//      )

    eventData.keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .fold (
        ("", 0L, 0),
        (acc: (String, Long, Int), r: (String, String, String, String)) => { ("", 0L, acc._3 + 1) },
        ( key: String,
          window: TimeWindow,
          counts: Iterable[(String, Long, Int)],
          out: Collector[(String, Long, Int)] ) =>
        {
          val count = counts.iterator.next()
          out.collect((key, window.getEnd, count._3))
        }
      )


    eventData.print("aaa")

    env.execute("KafkaDeserSchemaTest")

  }

  case class SensorsEvent(
                           _hybrid_h5: Boolean,
                           _track_id: Long,
                           event: String,
                           time: Long,
                           _flush_time: Long,
                           distinct_id: String,
                           lib: String,
                           properties: String,
                           `type`: String,
                           map_id: String,
                           user_id: Long,
                           recv_time: Long,
                           extractor: String,
                           project_id: Long,
                           project: String,
                           ver: Long)

}
