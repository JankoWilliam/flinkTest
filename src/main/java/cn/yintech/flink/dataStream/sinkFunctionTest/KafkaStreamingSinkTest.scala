package cn.yintech.flink.dataStream.sinkFunctionTest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema


object KafkaStreamingSinkTest {
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

    val kafkaSource = new FlinkKafkaConsumer011[ObjectNode](topic,new JSONKeyValueDeserializationSchema(true),properties)

    val dataStream = env.addSource(kafkaSource)
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

//    val eventData = dataStream.map(obj => {
//      val event = obj.get("value").get("event")
//      val age = obj.get("value").get("age")
//      val userID = obj.get("value").get("properties").get("userID")
//      val offset = obj.get("metadata").get("offset")
//      val topic = obj.get("metadata").get("topic")
//      val partition = obj.get("metadata").get("partition")
//      (event,age,userID,s"消费的主题是：$topic,分区是：$partition,当前偏移量是：$offset")
//    })
//
//    eventData.print("aaa")
    val sinkRow = StreamingFileSink
      .forRowFormat(new Path("D:\\idea_out\\rollfilesink"), new SimpleStringEncoder[ObjectNode]("UTF-8"))
      .withBucketAssigner(new DayBucketAssigner)
      // .withBucketCheckInterval(60 * 60 * 1000l) // 1 hour
      .build()

    // use define BulkWriterFactory and DayBucketAssinger
    val sinkBuck = StreamingFileSink
      .forBulkFormat(new Path("D:\\idea_out\\rollfilesink"), new DayBulkWriterFactory)
      .withBucketAssigner(new DayBucketAssigner())
      .build()

    dataStream.addSink(sinkBuck)

    env.execute("KafkaStreamingSinkTest")

  }


}
