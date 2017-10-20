package com.dt.spark.main.BasicLearn

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hjw on 16/7/17.
  */
object WordCnt {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./src/com/dt/spark/main/WordCnt.scala")


    val words = lines.flatMap { line => line.split(" ") }

    val pairs = words.map { word => (word, 1) }

    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))



    sc.stop()


  }

}
