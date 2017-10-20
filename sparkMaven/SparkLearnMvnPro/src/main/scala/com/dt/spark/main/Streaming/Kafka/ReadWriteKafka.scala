package com.dt.spark.main.Streaming.Kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hjw on 17/5/3.
  * https://github.com/apache/spark/tree/v2.1.0/examples/src/main
  * Usage: ReadWriteKafka <zkQuorum> <group> <topics> <numThreads>
  * <zkQuorum> is a list of one or more zookeeper servers that make quorum
  * <group> is the name of kafka consumer group
  * <topics> is a list of one or more kafka topics to consume from
  * <numThreads> is the number of threads the kafka consumer should use
  */
object ReadWriteKafka {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val path = "pathofcheckpoint"
    def createNewStreaming(): StreamingContext = {
      val conf = new SparkConf().setAppName("SparkStreamingKafka")
      val ssc = new StreamingContext(conf, Seconds(60))
      val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      val words = kafkaStream.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1L))
        .reduceByKeyAndWindow(_ + _, _ - _, Seconds(100), Seconds(20), 2)
      wordCounts.print()
      ssc.checkpoint(path) //进行 checkpoint
      ssc
    }
    //从已有 checkpoint 文件恢复或者创建 StreamingContext，最后一个参数表示从 checkpoint 文件恢复时如果出错是否直接创建
    val ssc = StreamingContext.getOrCreate(path, createNewStreaming, createOnError = true)
    ssc.start()
    ssc.awaitTermination()
  }
}
