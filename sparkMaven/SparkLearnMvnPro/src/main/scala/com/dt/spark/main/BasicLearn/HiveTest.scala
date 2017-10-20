package com.dt.spark.main.BasicLearn

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by hjw on 16/8/14.
  */


object HiveTest {

  case class KV(key: Int, value:String)

  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    //val kvRdd = sc.parallelize((1 to 100).map(i => KV(i,s"val_$i")))

    import sqlContext.implicits._

    val kvRdd = sc.parallelize((1 to 100).map(i => KV(i,s"val_$i"))).toDF()

    kvRdd.where('key>=1).where('key<=5).registerTempTable("kvrdd")

    val resultRDD = sqlContext.sql("select key, value from kvrdd").collect()

    resultRDD.foreach(row => {
      val f0 = if(row.isNullAt(0)) "null" else row.getInt(0)
      val f1 = if(row.isNullAt(1)) "null" else row.getString(1)
      println(s"result:$f0, $f1")
    })

    sc.stop()


  }

}
