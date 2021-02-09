package cn.yintech.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
 
import org.apache.hadoop.conf.Configuration
import org.apache.flink.api.scala._


 
//读取hive的数据
object FlinkReadHive1 {
  def main(args: Array[String]): Unit = {
 
//      val conf = new Configuration()
//      conf.set("hive.metastore.local", "false")
//
//      conf.set("hive.metastore.uris", "thrift://172.10.4.141:9083")
//       //如果是高可用 就需要是nameserver
////      conf.set("hive.metastore.uris", "thrift://172.10.4.142:9083")
//
//      val env = ExecutionEnvironment.getExecutionEnvironment
//
//      //todo 返回类型
//      val dataset: DataSet[TamAlert] = env.createInput(new HCatInputFormat[TamAlert]("aijiami", "test", conf))
//
//      dataset.first(10).print()
////      env.execute("flink hive test")
 
 
  }
 
}
