package cn.yintech.flink.dataStream

import java.beans.Transient
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import cn.yintech.flink.table.HBaseRichOutputFormat2
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.{CountEvictor, TimeEvictor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, SimpleStringSchema}
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


object KafkaPvUvCount {

  private final val log = LoggerFactory.getLogger(KafkaPvUvCount.getClass)

  def main(args: Array[String]): Unit = {

    //    val conf = new Configuration()
    //    import org.apache.flink.configuration.ConfigConstants
    //    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    //    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "LiveVisitCountFlink03")

    val topic = "sc_md"

    val kafkaSource = new FlinkKafkaConsumer011(topic, new SimpleStringSchema(), properties)
    //    kafkaSource.setStartFromTimestamp(1614528000000L)

    val dataStream = env.addSource(kafkaSource)
      .map(record => {
        val dataMap = jsonParse(record)
        (dataMap.getOrElse("event", ""),
          dataMap.getOrElse("distinct_id", ""),
          dataMap.getOrElse("properties", ""),
          dataMap.getOrElse("time", "").toLong)
      })
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, String, Long)](Time.seconds(30)) {
//        override def extractTimestamp(element: (String, String, String, Long)): Long = element._4
//      })
      .assignTimestampsAndWatermarks(new TimeStampExtractor)
      .setParallelism(3)

    val appStartData = dataStream
      //      .filter(v => v.get("value").get("event") != null && v.get("value").get("event").asText() == "$AppStart" )
      .filter(v => v._1 != null && v._1 == "$AppStart")
      .map(v => (v._2, v._4))
      //      .keyBy(_._1)
      .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
      //      .window(TumblingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
      .allowedLateness(Time.seconds(30))
      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
      .evictor(CountEvictor.of(0, true))

      //      .process(new ProcessWindowFunction[(String, Long), (String, String, Long), String, TimeWindow] {
      //        /*
      //        这是使用state是因为，窗口默认只会在创建结束的时候触发一次计算，然后数据结果，
      //        如果长时间的窗口，比如：一天的窗口，要是等到一天结束在输出结果，那还不如跑批。
      //        所有大窗口会添加trigger，以一定的频率输出中间结果。
      //        加evictor 是因为，每次trigger，触发计算是，窗口中的所有数据都会参与，所以数据会触发很多次，比较浪费，加evictor 驱逐已经计算过的数据，就不会重复计算了
      //        驱逐了已经计算过的数据，导致窗口数据不完全，所以需要state 存储我们需要的中间结果
      //         */
      //        var wordState: MapState[String, String] = _
      //        var pvCount: ValueState[Long] = _
      //
      //        override def open(parameters: Configuration): Unit = {
      //          // new MapStateDescriptor[String, String]("word", classOf[String], classOf[String])
      //          wordState = getRuntimeContext.getMapState(new MapStateDescriptor[String, String]("word", classOf[String], classOf[String]))
      //          pvCount = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("pvCount", classOf[Long]))
      //        }
      //
      //        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, String, Long)]): Unit = {
      //
      //
      //          var pv = 0L;
      //          val elementsIterator = elements.iterator
      //          // 遍历窗口数据，获取唯一word
      //          while (elementsIterator.hasNext) {
      //            pv += 1
      //            val word = elementsIterator.next()._1
      //            wordState.put(word, null)
      //          }
      //          // add current
      //          pv += pv + pvCount.value()  // fix bug: pv value not add pvCount in state
      //          pvCount.update(pv)
      //          var count: Long = 0
      //          val wordIterator = wordState.keys().iterator()
      //          while (wordIterator.hasNext) {
      //            wordIterator.next()
      //            count += 1
      //          }
      //          // uv
      //          out.collect((key, "uv", count))
      //          out.collect(key, "pv", pv)
      //
      //        }
      //      })

      .process(
        new ProcessAllWindowFunction[(String, Long), (String, String, String, String), TimeWindow] {

          // 自定义状态管理(我们自定义的这些状态，都是Keyed State)
          @transient // 不要序列化
          var uvState: MapState[String, String] = _
          @transient
          var pvState: ValueState[Int] = _

          // 存储窗口开始时间戳，用以清空状态，从头计算
          @Transient
          var windowStartState: ValueState[Long] = _

          override def open(parameters: Configuration): Unit = {

            val uvStateDesc: MapStateDescriptor[String, String] = new MapStateDescriptor[String, String]("uv", classOf[String], classOf[String])
            val pvStateDesc: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("pv", classOf[Int])

            val windowStartStateDesc: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long]("windowStart", classOf[Long])

            // 1、创建TTLConfig (过期状态清除)
            val ttlConfig: StateTtlConfig = StateTtlConfig
              .newBuilder(org.apache.flink.api.common.time.Time.days(2)) // 这是state的存活时间, 保存两天的状态
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //设置过期时间更新方式
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //永远不要返回过期的状态
              // .cleanupInRocksdbCompactFilter(1000)//处理完1000个状态查询时候，会启用一次CompactFilter
              .build()

            // 2、开启TTL
            uvStateDesc.enableTimeToLive(ttlConfig)
            pvStateDesc.enableTimeToLive(ttlConfig)

            uvState = this.getRuntimeContext.getMapState(uvStateDesc)
            pvState = this.getRuntimeContext.getState(pvStateDesc)

            windowStartState = this.getRuntimeContext.getState(windowStartStateDesc)
          }

          def process(context: Context,
                      elements: Iterable[(String, Long)],
                      out: Collector[(String, String, String, String)]): Unit = {
            val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
            // 计算状态初始化
            val windowStart: Long = windowStartState.value()
            // 流的第一条数据，触发
            if (windowStart == 0) {
              windowStartState.update(context.window.getStart)
            }
            // 跨窗口，clear状态, 更新 windowStartState
            if (windowStart < context.window.getStart) {

              println(s"pv&uv last windowStartState: ${sdf1.format(new Date(windowStart))}, current windowStartState : ${sdf1.format(new Date(context.window.getStart))} State across days, clear calculates state...")
              log.info(s"pv&uv last windowStartState: ${sdf1.format(new Date(windowStart))}, current windowStartState : ${sdf1.format(new Date(context.window.getStart))} State across days, clear calculates state...")
              // 窗口开始时间戳
              // val start: Long = context.window.getStart
              // 窗口结束时间戳
              // val end: Long = context.window.getEnd
              // println(s"开始 start: $start  结束 end: $end ")

              // 我这里的状态按理来说，每天计算都会执行一个清空的操作，应该不需要TTL的操作
              // 需要探索一下状态clear 和 TTL 的区别？？？
              pvState.clear()
              uvState.clear()
              // 更新为新的窗口状态
              windowStartState.update(context.window.getStart)
            }

            var pv: Int = 0

            val iterator = elements.iterator
            while (iterator.hasNext) {

              pv = pv + 1
              val userClick = iterator.next()
              val userId: String = userClick._1
              uvState.put(userId, null)
            }

            var uv: Int = 0
            val uvIterator = uvState.keys().iterator()
            while (uvIterator.hasNext) {
              val next: String = uvIterator.next()
              uv = uv + 1
            }

            val value: Int = pvState.value()

            //            println(s"pv 状态初始值: $value")
            // 这里的状态的初始默认值为0, 对于scala里面的Int不需要做判空操作，初始值就是0
            if (value == 0) {
              pvState.update(pv)
            } else {
              pvState.update(value + pv)
            }
            //
            //            out.collect((s"数据日期: ${sdf.format(new Date(windowStart))}~${sdf.format(new Date(context.window.maxTimestamp()))}", "流量统计pv", pvState.value()))
            //            out.collect((s"数据日期: ${sdf.format(new Date(windowStart))}~${sdf.format(new Date(context.window.maxTimestamp()))}", "流量统计uv", uv))


            out.collect((sdf2.format(new Date(context.window.getStart)), "pv", sdf1.format(new Date()), pvState.value().toString))
            out.collect((sdf2.format(new Date(context.window.getStart)), "uv", sdf1.format(new Date()), uv.toString))
          }


        })

    val liveData = dataStream
      //      .filter(v => v.get("value").get("event") != null && v.get("value").get("event").asText() == "$AppStart" )
      .filter(v => v._1 != null && v._1 == "LiveVisit")
      .map(v => {
        val dataMap = jsonParse(v._3)
        (v._2, v._4, dataMap.getOrElse("v1_message_id", "null"))
      })

    val liveDataSum = liveData.map(v => v._1)
      //      .keyBy(_._1)
      .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
      .allowedLateness(Time.seconds(30))
      //      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
      .evictor(CountEvictor.of(0, true))
      .process(new ProcessAllWindowFunction[String, (String, String, String, String), TimeWindow] {

        // 自定义状态管理(我们自定义的这些状态，都是Keyed State)
        @transient // 不要序列化
        var userLiveMap: MapState[String, Int] = _
        // 存储窗口开始时间戳，用以清空状态，从头计算
        @Transient
        var windowStartState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val uvStateDesc: MapStateDescriptor[String, Int] = new MapStateDescriptor[String, Int]("userLive", classOf[String], classOf[Int])
          val windowStartStateDesc: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long]("windowStart", classOf[Long])

          // 1、创建TTLConfig (过期状态清除)
          val ttlConfig: StateTtlConfig = StateTtlConfig
            .newBuilder(org.apache.flink.api.common.time.Time.days(2)) // 这是state的存活时间, 保存两天的状态
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //设置过期时间更新方式
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //永远不要返回过期的状态
            // .cleanupInRocksdbCompactFilter(1000)//处理完1000个状态查询时候，会启用一次CompactFilter
            .build()

          // 2、开启TTL
          uvStateDesc.enableTimeToLive(ttlConfig)

          userLiveMap = this.getRuntimeContext.getMapState(uvStateDesc)
          windowStartState = this.getRuntimeContext.getState(windowStartStateDesc)
        }

        override def process(context: Context, elements: Iterable[String], out: Collector[(String, String, String, String)]): Unit = {
          val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
          // 计算状态初始化
          val windowStart: Long = windowStartState.value()
          // 流的第一条数据，触发
          if (windowStart == 0) {
            windowStartState.update(context.window.getStart)
          }
          // 跨窗口，clear状态, 更新 windowStartState
          if (windowStart < context.window.getStart) {

            println(s"userLive last windowStartState: ${sdf1.format(new Date(windowStart))}, current windowStartState : ${sdf1.format(new Date(context.window.getStart))} State across days, clear calculates state...")
            log.info(s"userLive last windowStartState: ${sdf1.format(new Date(windowStart))}, current windowStartState : ${sdf1.format(new Date(context.window.getStart))} State across days, clear calculates state...")
            // 窗口开始时间戳
            // val start: Long = context.window.getStart
            // 窗口结束时间戳
            // val end: Long = context.window.getEnd
            // println(s"开始 start: $start  结束 end: $end ")

            // 我这里的状态按理来说，每天计算都会执行一个清空的操作，应该不需要TTL的操作
            // 需要探索一下状态clear 和 TTL 的区别？？？
            userLiveMap.clear()
            // 更新为新的窗口状态
            windowStartState.update(context.window.getStart)
          }


          for (element <- elements.iterator) {
            if (userLiveMap.contains(element))
              userLiveMap.put(element, userLiveMap.get(element) + 1)
            else
              userLiveMap.put(element, 1)
          }
          var sum = 0
          var count = 0
          val value = userLiveMap.values().iterator()
          while (value.hasNext) {
            sum += value.next()
            count += 1
          }
          val result = sum * 3.0 / count / 60
          //          out.collect((s"数据日期: ${sdf.format(new Date(context.window.getStart))}~${sdf.format(new Date(context.window.maxTimestamp()))}", "直播统计平均时长（分钟）", result))
          out.collect((sdf2.format(new Date(context.window.getStart)), "live_avg", sdf1.format(new Date()), result.toString))
          out.collect((sdf2.format(new Date(context.window.getStart)), "live_users", sdf1.format(new Date()), count.toString))
        }
      })



    //      .timeWindowAll(Time.seconds(5))
    //      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
    //      .evictor(TimeEvictor.of(Time.seconds(0), true))
    //      .process(new ProcessAllWindowFunction[(String, Int), (String, String, Double), TimeWindow] {
    //        override def process(context: Context,
    //                             elements: Iterable[(String, Int)],
    //                             out: Collector[(String, String, Double)]): Unit = {
    //          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //          val sum = elements.map(_._2).sum
    //          print("sum:"+sum)
    //          val result = sum * 3.0 / elements.size / 60
    //          out.collect((s"数据日期: ${sdf.format(new Date(context.window.getStart))}~${sdf.format(new Date(context.window.maxTimestamp()))}", "直播统计平均时长（分钟）", result))
    //
    //        }
    //      })
    //      .sum(1)
    //      .print("bbb")


    appStartData.writeUsingOutputFormat(new HBaseRichOutputFormat2)
    liveDataSum.writeUsingOutputFormat(new HBaseRichOutputFormat2)

    env.execute("KafkaPvUvCount")

  }

  class TimeStampExtractor extends AssignerWithPeriodicWatermarks[(String, String, String, Long)] with Serializable {

    val maxOutOfOrderness = 30000L
    var currentMaxTimestamp: Long = _
    var a: Watermark = null

    override def getCurrentWatermark: Watermark = {
      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      a
    }

    override def extractTimestamp(t: (String, String, String, Long), l: Long): Long = {

      val timestamp = t._4
      val nowTime = new Date().getTime
      if ((timestamp - nowTime) > 600000) { // 若时间戳比当前时间还要大的话，取上一次的时间戳
        currentMaxTimestamp
      } else {
        currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }

  }

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try {
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        val outKey = outIter.next()
        val outValue = if (outJsonObj.get(outKey) != null) outJsonObj.get(outKey).toString else "null"
        map += (outKey -> outValue)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    map
  }
}
