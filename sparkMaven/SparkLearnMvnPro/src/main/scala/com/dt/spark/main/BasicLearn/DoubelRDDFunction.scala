package com.dt.spark.main.BasicLearn

import org.apache.spark.{SparkConf, SparkContext}



// 备注:在本地是有些函数提示找不到但是仍可以运行,比如 histogram
/**
  * Created by hjw on 16/11/27.
  */
object DoubelRDDFunction {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val hgRDD = sc.parallelize(List(1.1, 1.2, 2.1, 2.2, 2, 3, 4.1,4.3, 7.1, 8.3, 9.3), 2)
    val arr:Array[Double] = Array(0.0,4.1,9.0)
    val res = hgRDD.histogram(arr) //.histogram(Array(0.0,4.1,9.0))
    res.foreach(println)
//    6
//    4
    sc.stop()


  }

}
