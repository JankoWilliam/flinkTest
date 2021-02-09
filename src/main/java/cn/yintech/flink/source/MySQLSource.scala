package cn.yintech.flink.source

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import cn.yintech.flink.source.FlinkBean.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MySQLSource extends RichSourceFunction[SensorReading] {
  var conn:Connection = null
  var ps:PreparedStatement = null
  // 流打开时操作
  override def open(parameters: Configuration): Unit = {
    // 加载驱动
    Class.forName("com.mysql.jdbc.Driver")
    // 数据库连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8&useUnicode=true&useSSL=false&serverTimezone=GMT","root","root")
    ps = conn.prepareStatement("select * from flink_sensorreading where c_time > '2021-01-26'")
  }

  // 流运行时操作
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    try {
      var resultSet:ResultSet = ps.executeQuery()
      while (resultSet.next()){
        var id:String = resultSet.getString("id")
        var curTime:Long = resultSet.getLong("timestamp")
        var timepreture:Double = resultSet.getDouble("timepreture")
        sourceContext.collect(SensorReading(id,curTime,timepreture))
      }
    } catch {
      case _:Exception => 0
    } finally {
      conn.close()
    }

  }
  
  // 流关闭时操作
  override def cancel(): Unit = {
    try{
      if(conn!=null){
        conn.close()
      }
      if(ps!=null){
        ps.close()
      }
    } catch {
      case _:Exception => print("error")
    }
  }
}