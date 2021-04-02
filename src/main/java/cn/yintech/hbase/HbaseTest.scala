package cn.yintech.hbase

import cn.yintech.utils.HbaseUtil
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object HbaseTest {
  def main(args: Array[String]): Unit = {
    val table: Table = HbaseUtil().connect.getTable(TableName.valueOf("flink_live_visit_count_sink"))

    val get = new Get("待我发起进攻总号角+61260".getBytes) // 根据rowkey查询
    val result = table.get(get)
    System.out.println("获得到rowkey:" + new String(result.getRow))
    import scala.collection.JavaConversions._
    for (cell <- result.rawCells()) {
      System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)))
      System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)))
      System.out.println(Bytes.toString(CellUtil.cloneValue(cell)))
    }

  }
}
