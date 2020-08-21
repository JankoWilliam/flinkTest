package cn.yintech.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation

class HbaseUtil(getConnect:()=> Connection) extends Serializable {
  lazy val  connect = getConnect()
}

object HbaseUtil {
  val conf: Configuration = HBaseConfiguration.create
  conf.set("hbase.zookeeper.quorum", "bigdata002,bigdata003,bigdata004")
//  conf.set("zookeeper.znode.parent", "/hbase-unsecure")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.setLong("hbase.rpc.timeout", 3000000L)
//  conf.setInt("hbase.client.ipc.pool.size", 1)
  // No FileSystem for schema : hdfs
  conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

  def apply(): HbaseUtil = {
    val f = ()=>{
      UserGroupInformation.setConfiguration(conf)
      val romoteUser = UserGroupInformation.createRemoteUser("hbase")
      UserGroupInformation.setLoginUser(romoteUser)
      val connection = ConnectionFactory.createConnection(conf)
      //释放资源 在executor的JVM关闭之前,千万不要忘记
      sys.addShutdownHook {
        connection.close()
      }
      connection
    }
    new HbaseUtil(f)
  }
}
