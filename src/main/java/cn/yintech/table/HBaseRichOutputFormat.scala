package cn.yintech.table

import cn.yintech.utils.HbaseUtil
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

class HBaseRichOutputFormat extends RichOutputFormat[Row]{
  val logger: Logger = LoggerFactory.getLogger(getClass)
  var table: Table = _

  override def configure(parameters: Configuration) :Unit= {
    logger.info("configure open")
  }
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    table = HbaseUtil().connect.getTable(TableName.valueOf("flink_live_visit_count_sink"))
  }
  override def writeRecord(record: Row): Unit ={
    import scala.collection.JavaConverters._
    //批量写入数据
//    val list = record.map(d=>{
//      val put = new Put(Bytes.toBytes(d._1))
//      put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("name"),Bytes.toBytes(d._2))
//      put
//    }).toList
    val put = new Put(Bytes.toBytes(record.getField(1).toString + "+" + record.getField(0).toString))
    put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("uv"),Bytes.toBytes(record.getField(3).toString))
    put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("max_online"),Bytes.toBytes("0"))
    table.put(put)
  }
  override def close()  :Unit= {
    // 结束的时候记得关闭连接（其实永远不会结束）
    table.close()
  }
}
