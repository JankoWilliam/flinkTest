package cn.yintech.table

import java.io.Serializable
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import cn.yintech.utils.ScalaUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.{EnvironmentSettings, Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object FlinkTableKafkaTest2 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getConfig.setAutoWatermarkInterval(5000)
    val bsSettings = EnvironmentSettings.newInstance.useOldPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092")
//    properties.setProperty("auto.offset.reset", "latest")
//    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "LiveVisitCountFlink04")
    val topic = "sc_md"

    val myConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), properties).setStartFromGroupOffsets()
    //指定偏移量
    //    myConsumer.setStartFromEarliest() //从最早的数据开始
    myConsumer.setStartFromLatest() //从最新的数据开始
    val stream = env.addSource(myConsumer).assignTimestampsAndWatermarks(new TimeStampExtractor)
    //      .assignAscendingTimestamps(jsonParse(_).getOrElse("time", "0").toLong)
    //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
    //        override def extractTimestamp(t: String): Long = {
    //          //返回指定的字段作为水印字段，这里设置为10秒延迟
    //          jsonParse(t).getOrElse("time", "0").toLong
    //        }
    //      })

    val value: DataStream[(String, String, String, String, Long)] = stream.map(record => {
      val dataMap = jsonParse(record)
      (dataMap.getOrElse("event", ""), dataMap.getOrElse("properties", ""), dataMap.getOrElse("time", "").toLong, dataMap.getOrElse("distinct_id", ""))
    })
      .filter(_._1 == "LiveVisit")
      .map(v => {
        val dataMap2 = jsonParse(v._2)
        (dataMap2.getOrElse("v1_message_id", ""),
          dataMap2.getOrElse("v1_element_content", ""),
          dataMap2.getOrElse("v1_message_title", ""),
          v._4,
          v._3)
      })
      .filter(v => v._2 == "视频直播播放" && v._1 != "" && v._4 != "")


    tableEnv.registerFunction("utc2local", new FlinkTableKafkaTest.UTC2Local)


    val liveVisitTable = tableEnv.fromDataStream(value, 'v1_message_id, 'v1_element_content, 'v1_message_title, 'distinct_id, 'time.rowtime)
    tableEnv.registerTable("live_visit",liveVisitTable)


    val sql = "SELECT v1_message_id,v1_message_title," +
            "utc2local(HOP_START(`time`, INTERVAL '10' SECOND,INTERVAL '1' DAY)) as hStart," +
            "utc2local(HOP_END(`time`, INTERVAL '10' SECOND,INTERVAL '1' DAY)) as hEnd," +
            "count(DISTINCT distinct_id) AS countNum " +
            "from live_visit where v1_message_id is not null " +
            "GROUP BY HOP(`time`,INTERVAL '10' SECOND,INTERVAL '1' DAY),v1_message_id,v1_message_title"


    val table: Table = tableEnv.sqlQuery(sql)
    val result: DataStream[Row] = tableEnv.toAppendStream[Row](table)
    result.print()
    //写入hbase
//    result.writeUsingOutputFormat(new HBaseRichOutputFormat())

    env.execute("FlinkTableKafkaTest2")
  }


  class UTC2Local extends ScalarFunction with Serializable {
    def eval(s: Timestamp): Timestamp = {
      val timestamp = s.getTime + 28800000
      new Timestamp(timestamp)
    }

    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = Types.SQL_TIMESTAMP
  }

  class TimeStampExtractor extends AssignerWithPeriodicWatermarks[String] with Serializable {

    val maxOutOfOrderness = 30000L // 3.5 seconds
    var currentMaxTimestamp: Long = _
    var a: Watermark = null

    override def getCurrentWatermark: Watermark = {
      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      a
    }

    override def extractTimestamp(t: String, l: Long): Long = {

      val timestamp = jsonParse(t).getOrElse("time", null).toLong
      //      val timestamp = new Date().getTime
      currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
      //      println(t)
      //      println("timestamp:" + sdf.format(timestamp) + "|currentMaxTimestamp:" + sdf.format(currentMaxTimestamp) + "|Watermark:" + a.toString+"--"+sdf.format(a.getTimestamp) + "|systime:" + sdf.format(new Date) )
      //      println("timestamp:" + sdf.format(timestamp) + "|currentMaxTimestamp:" + sdf.format(currentMaxTimestamp) + "|Watermark:" + a.toString+"--"+sdf.format(a.getTimestamp) )
      timestamp
    }

  }
}
