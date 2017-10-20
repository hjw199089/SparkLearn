package com.dt.spark.main.BasicLearn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjw on 16/7/19.
  */
object QuickStartBasic {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./srcFile/QuickStartBasic.txt")

    val linesCountNum = lines.count()

    val linesWithSpark = lines.filter(lines => lines.contains("spark"))

    //Let’s say we want to find the line with the most words
    val maxsize = lines.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)

    val maxsize_math = lines.map(lines => lines.split(" ").size).reduce((a, b) => Math.max(a, b))

    //compute the per-word counts
    val wordCounts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

    //Array[(String, Int)] = Array((means,1), (under,2), (this,3), (Because,1), (Python,2), (agree,1), (cluster.,1), ...)
    wordCounts.collect().foreach(print)
    //(spark,13)(hjw,6)

    println("linesCountNum = " + linesCountNum)
    print("lineslinesWithSpark")
    linesWithSpark.foreach(println)
    println("maxsize = " + maxsize)
    println("maxsize_math = " + maxsize_math)
    print("wordCounts")
    wordCounts.foreach(println)



    //      spark spark
    //      spark spark
    //      spark spark
    //      spark spark
    //      spark spark
    //      spark spark spark
    //      maxsize = 3
    //    (spark, 13)
    //    (hjw, 6)


    sc.stop()


  }

}
