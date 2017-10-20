package com.dt.spark.main.DataFrameLearn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-DataFrame学习记录-[3]以Json字符串构建RDD转DF
（1）字符串中$闭包自由变值
（2）以Json字符串构建RDD转DF
  */
object DataFrameSQL_3 {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sqlContext = new HiveContext(sc)

    val name = "yourName"
    val age  = 12
    val tesRDD = sc.makeRDD(s"""{"num":"$name","age":"$age"}""".stripMargin:: Nil)
    val tesRDD2DF = sqlContext.read.json(tesRDD)
    tesRDD2DF.show()
    //    +---+--------+
    //    |age|     num|
    //    +---+--------+
    //    | 12|yourName|
    //    +---+--------+

    sc.stop()

  }

}
