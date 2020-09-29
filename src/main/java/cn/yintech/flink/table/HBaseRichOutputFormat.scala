package cn.yintech.flink.table

import cn.yintech.utils.HbaseUtil
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
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
    val rowKey = record.getField(1).toString + "+" + record.getField(0).toString
    val get = new Get(Bytes.toBytes(rowKey))
    get.addColumn("cf1".getBytes, "uv".getBytes)
    val result: Result = table.get(get)
    var uv = 0
    for (cell <- result.rawCells()) {
      if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("uv"))
        uv = Bytes.toString(CellUtil.cloneValue(cell)).toInt
    }

    val record_uv = record.getField(4).toString.toInt
    println("record_uv实际值：" + record_uv + " 当前uv值："+uv)

    if (record_uv > uv){
      println("插入数据...")
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("uv"),Bytes.toBytes(record_uv.toString))
      put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("max_online"),Bytes.toBytes("0"))
      table.put(put)
    }
  }
  override def close()  :Unit= {
    // 结束的时候记得关闭连接（其实永远不会结束）
    table.close()
  }
}
