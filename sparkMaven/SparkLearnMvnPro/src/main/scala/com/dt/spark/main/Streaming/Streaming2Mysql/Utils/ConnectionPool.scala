package com.dt.spark.main.Streaming.Streaming2Mysql.Utils

import java.sql.{Statement, Connection, DriverManager}

import org.apache.log4j.Logger

/**
  * Created by hjw on 17/5/4.
  */
object ConnectionPool {
  val  log = Logger.getLogger(ConnectionPool.this.getClass)
  def mysqlExe(sql: String) {
    var conn: Connection = null
    val url: String = "jdbc:mysql://localhost:3306/spark_stream?" + "user=root&password=123&useUnicode=true&characterEncoding=UTF8"
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(url)
      val stmt: Statement = conn.createStatement
      stmt.executeUpdate(sql)
    }
    catch {
      case e: Exception => {
        e.printStackTrace
      }
    } finally {
      try {
        conn.close
      }
      catch {
        case e: Exception => {
        }
      }
    }
  }
}