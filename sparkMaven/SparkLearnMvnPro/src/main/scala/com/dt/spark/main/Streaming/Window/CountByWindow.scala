
package com.dt.spark.main.Streaming.Window

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._ // not necessary since Spark 1.3

object CountByWindow {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit ={
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))


    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    ssc.checkpoint("CountByWindow_checkpoint")
    // Split each line into words
    val words = lines.flatMap(_.split(" ")) // not necessary since Spark 1.3 // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)


    val windowedWordCounts = pairs.countByWindow( Seconds(3), Seconds(2))


    // Print the first ten elements of each RDD generated in this DStream to the console
      windowedWordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
