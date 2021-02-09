package cn.yintech.flink.source

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object MysqlUtil {
  def main(args: Array[String]): Unit = {
    val list:  List[ JSONObject] = queryList("select * from sensor limit 5")
    println(list)
  }

  def queryList(sql:String):List[JSONObject]={
    //加载驱动
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[ JSONObject]()
    //链接数据库
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/gmall1122?useSSL=false","root","123456")
    val stat: Statement = conn.createStatement
    val rs: ResultSet = stat.executeQuery(sql )
    val md: ResultSetMetaData = rs.getMetaData
    while (  rs.next ) {
      val rowData = new JSONObject();
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList

    //
  }

}