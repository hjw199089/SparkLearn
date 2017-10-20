package com.dt.spark.main.DataFrameLearn

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjw on 17/3/25.
  */
//http://stackoverflow.com/questions/30218140/spark-how-to-translate-countdistinctvalue-in-dataframe-apis#
object DataFrameSQL_agg {

  case class KV(page: String, visitor: String)

  case class Logs(city_id: String, ctype: String, uuid: String)

  def main(args: Array[String]) {
    val list = List(("PAG1", "V1"),
      ("PAG1", "V1"),
      ("PAG2", "V1"),
      ("PAG2", "V2"),
      ("PAG2", "V1"),
      ("PAG1", "V1"),
      ("PAG1", "V2"),
      ("PAG1", "V1"),
      ("PAG1", "V2"),
      ("PAG1", "V1"),
      ("PAG2", "V2"),
      ("PAG1", "V3"))

    val list2 = List(
      ("1001", "android", "V1"),
      ("1001", "android", "V1"),
      ("1001", "android", "V2"),
      ("1001", "iphone", "V1"),
      ("1001", "iphone", "V1"),
      ("1002", "android", "V1"),
      ("1002", "android", "V1"),
      ("1002", "android", "V2"),
      ("1002", "iphone", "V1"),
      ("1002", "iphone", "V1")
    )



    val list3 = List(
      ("1001", "android", "V1"),
      ("1001", "android", "V1"),
      ("1001", "android", "V2"),
      ("1001", "android", "V1"),
      ("1001", "android", "V1"),
      ("1002", "android", "V1"),
      ("1002", "android", "V1"),
      ("1002", "android", "V2"),
      ("1002", "iphone", "V1"),
      ("1002", "iphone", "V1")
    )


    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val logs = sc.parallelize(list.map(iter => KV(iter._1, iter._2))).toDF

    //======方法一:SQL===============
    logs.registerTempTable("logs")
    val sqlResult = sqlContext.sql(
      """
       select
         page,
         visitor  as vs
       from(
       select
       page,
       count(distinct visitor) as visitor
       from logs
        group by page
        )A
      """)
    val result = sqlResult.map(x => (x(0).toString, x(1).toString))
    result.foreach(println)
    //    (PAG1,3)
    //    (PAG2,2)

    //======方法一:API===============
    //df.groupby("a").agg()
    //df.select('a).distinct.count
    //df.groupBy("word").agg(countDistinct("title")).show()
    val result2 = logs.select("page", "visitor").groupBy('page).agg('page, countDistinct('visitor))
    result.foreach(println)



    val Log = sc.parallelize(list2.map(iter => Logs(iter._1, iter._2, iter._3))).toDF

    println("==========test3=========")
    val result3 = Log.groupBy('city_id,'ctype).agg(countDistinct('uuid))
    result3.foreach(println)
    //      [1001,android,2]
    //      [1001,iphone,1]
    //      [1002,iphone,1]
    //      [1002,android,2]

    println("==========test4=========")
    val result4 = Log.groupBy('city_id).agg(countDistinct('uuid))
    result4.foreach(println)
    //      [1001,2]
    //      [1002,2]

    println("==========test5=========")
    val Log3 = sc.parallelize(list3.map(iter => Logs(iter._1, iter._2, iter._3))).toDF
    val result5 = Log3.groupBy('city_id).agg(countDistinct('uuid),countDistinct('ctype))
    result5.foreach(println)
     //      [1002,2,2]
     //      [1001,2,1]


    sc.stop()
  }

}
