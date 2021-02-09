package cn.yintech.flink.source

import java.sql.{Connection, DriverManager, PreparedStatement}

import cn.yintech.flink.source.FlinkBean.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MySQLSink extends RichSinkFunction[SensorReading]{
  var conn:Connection = null
  var ps:PreparedStatement = null
  val INSERT_CASE:String = "INSERT INTO flink_sensorreading_sink (id, timestamp,timepreture) " + "VALUES (?, ?, ?) "

  override def open(parameters: Configuration): Unit = {
    // 加载驱动
    Class.forName("com.mysql.jdbc.Driver")
    // 数据库连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8&useUnicode=true&useSSL=false&serverTimezone=GMT","root","root")
    ps = conn.prepareStatement(INSERT_CASE)
  }

  override def invoke(value:SensorReading): Unit = {
    try{
      ps.setString(1,value.id)
      ps.setLong(2,value.timestamp)
      ps.setDouble(3,value.timepreture)
      ps.addBatch()
      ps.executeBatch()
    } catch {
      case _:Exception => 0
    }
  }
}