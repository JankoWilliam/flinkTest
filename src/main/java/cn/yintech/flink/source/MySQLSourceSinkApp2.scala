package cn.yintech.flink.source

import cn.yintech.flink.source.FlinkBean.SensorReading
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.types.Row

object MySQLSourceSinkApp2 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8&useUnicode=true&useSSL=false&serverTimezone=GMT"
    val username = "root"
    val password = "root"
    val sql_read = "select * from flink_sensorreading where c_time > '2021-01-26'"
    val sql_write = "insert into flink_sensorreading_sink (id, timestamp,timepreture) values(?,?,?)"


    def readMysql(env:ExecutionEnvironment,url: String, driver: String, user: String, pwd: String, sql: String): DataSet[SensorReading] ={
      // 获取数据流
      val dataResult: DataSet[Row] = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
        .setDrivername(driver)
        .setDBUrl(url)
        .setUsername(user)
        .setPassword(pwd)
        .setQuery(sql)
        .setRowTypeInfo(new RowTypeInfo(
          BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.LONG_TYPE_INFO,
          BasicTypeInfo.DOUBLE_TYPE_INFO))
        .finish())

      // 转化为自定义格式
      val dStream = dataResult.map(x=> {
        val id = x.getField(0).asInstanceOf[Int].toString
        val timestamp = x.getField(1).asInstanceOf[Long]
        val timepreture = x.getField(2).asInstanceOf[Double]
        SensorReading(id, timestamp, timepreture)
      })
      return dStream
    }

    // 读取mysql数据
    val readStream = readMysql(env, url, driver ,username ,password ,sql_read)

    // 将流中的数据格式转化为JDBCOutputFormat接受的格式
    val outputData = readStream.map(x => {
      val row = new Row(3)
      row.setField(0, x.id)
      row.setField(1, x.timestamp)
      row.setField(2, x.timepreture)
      row
    })




    def writeMysql(env: ExecutionEnvironment, outputData: DataSet[Row], url: String, user: String, pwd: String, sql: String): Unit = {
      outputData.output(JDBCOutputFormat.buildJDBCOutputFormat()
        .setDrivername("com.mysql.jdbc.Driver")
        .setDBUrl(url)
        .setUsername(user)
        .setPassword(pwd)
        .setQuery(sql)
        .finish())
      env.execute("insert data to mysql")
      print("data write successfully")
    }

    // 向mysql插入数据
    writeMysql(env,outputData,url,username,password,sql_write)

  }


}