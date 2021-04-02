package cn.yintech.flink.table

import cn.yintech.utils.HbaseUtil
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

class HBaseRichOutputFormat2 extends RichOutputFormat[(String,String,String,String)] {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  var table: Table = _

  override def configure(parameters: Configuration): Unit = {
    logger.info("configure open")
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    table = HbaseUtil().connect.getTable(TableName.valueOf("flink_live_visit_count_sink"))
  }

  override def writeRecord(record: (String,String,String,String)): Unit = {
    //批量写入数据
    //    val list = record.map(d=>{
    //      val put = new Put(Bytes.toBytes(d._1))
    //      put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("name"),Bytes.toBytes(d._2))
    //      put
    //    }).toList

    //(rowkey,列，u_time列，值)
    val rowKey = record._1

    val put = new Put(Bytes.toBytes(rowKey))
    // 值
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(record._2), Bytes.toBytes(record._4))
    // u_time
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("u_time"), Bytes.toBytes(record._3))
    table.put(put)
  }

  override def close(): Unit = {
    // 结束的时候记得关闭连接（其实永远不会结束）
    table.close()
  }
}
