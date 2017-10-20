package com.dt.spark.main.Streaming.Window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjw on 17/5/2.
  */
object CountByValueAndWindow {
  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))


    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    ssc.checkpoint("CountByValueAndWindow_checkpoint")
    // Split each line into words
    val words = lines.flatMap(_.split(" ")) // not necessary since Spark 1.3 // Count each word in each batch

    val countByValueAndWindow = words.countByValueAndWindow(Seconds(10), Seconds(2), 2)

    countByValueAndWindow.print()


    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
