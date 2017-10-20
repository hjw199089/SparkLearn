package com.dt.spark.main.Streaming.Streaming2Mysql

import com.dt.spark.main.Streaming.Streaming2Mysql.Utils.ConnectionPool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjw on 17/5/4.
  */
object Streaming2Mysql {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val path = "pathofcheckpoint"

    def NetworkWordStreaming(): StreamingContext = {
      val conf = new SparkConf().setMaster("local[5]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(conf, Seconds(1))
      // Create a DStream that will connect to hostname:port, like localhost:9999
      val lines = ssc.socketTextStream("localhost", 9999)

      // Split each line into words
      val words = lines.flatMap(_.split(" ")) // not necessary since Spark 1.3 // Count each word in each batch
      val pairs = words.map(word => (word, 1)).filter(!_.equals(""))
      val wordCounts = pairs.reduceByKey(_ + _)
      // Print the first ten elements of each RDD generated in this DStream to the console
      wordCounts.print()

      wordCounts.foreachRDD(rdd => {
        rdd.foreachPartition(partitionOfRecords => {
          partitionOfRecords.foreach(record => {
            ConnectionPool.mysqlExe(s"insert into streaming_keywordcnt_test(insert_time,keyword,count) values" +
              s" ('${System.currentTimeMillis()}', '${record._1}',${record._2} )")
          })
        }
        )
      }
      )
      //进行 checkpoint
      ssc.checkpoint(path)
      ssc
    }

    //从已有 checkpoint 文件恢复或者创建 StreamingContext，最后一个参数表示从 checkpoint 文件恢复时如果出错是否直接创建
    val ssc = StreamingContext.getOrCreate(path, NetworkWordStreaming, createOnError = true)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


}
