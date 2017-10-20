package com.dt.spark.main.DataFrameLearn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

case class Book(title:String ,words:String)
case class Word(word:String)
/**
  * Created by hjw on 16/7/17.
  */

//df.groupby("a").agg()
//df.select('a).distinct.count
//df.groupBy("word").agg(countDistinct("title")).show()


/**
  * explode
  */
object DataFrameSQL_explode {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    //加载txt文件
    val book = sc.textFile("./src/com/dt/spark/main/DataFrameLearn/srcFile/book.txt")


    //==========================================

    val df = book.map(_.split(",")).map(b=>Book(b(0),b(1))).toDF()
    val allWords = df.explode('words){case Row(words:String)=>words.split(" ").map(Word(_))}
    allWords.map{
      row => "0 = " + row(0) + "; 1 = " + row(1) + "; 2 = " + row(2)
    }.foreach(println(_))
    //    0 = book_1; 1 = I am book1; 2 = I
    //    0 = book_1; 1 = I am book1; 2 = am
    //    0 = book_1; 1 = I am book1; 2 = book1
    //    0 = book_2; 1 = red had; 2 = red
    //    0 = book_2; 1 = red had; 2 = had


    //import org.apache.spark.sql.functions._
    allWords.groupBy("word").agg(countDistinct("title")).show()
//    +-----+------------+
//    | word|count(title)|
//      +-----+------------+
//    |  had|           1|
//    |book1|           1|
//    |   am|           1|
//    |    I|           1|
//    |  red|           1|
//    +-----+------------+
      sc.stop()

  }

}
