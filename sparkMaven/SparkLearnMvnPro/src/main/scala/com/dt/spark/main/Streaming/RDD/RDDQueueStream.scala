package com.dt.spark.main.Streaming.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
/**
  * Created by hjw on 17/4/18.
  */

/*
Basic Sources
[1]File Streams:
For reading data from files on any file system compatible with the HDFS API (that is, HDFS, S3, NFS, etc.), a DStream can be created as:
streamingContext.fileStream[KeyClass, ValueClass, InputFormatClass](dataDirectory)
[2]Streams based on Custom Actors:
DStreams can be created with data streams received through Akka actors by using
 streamingContext.actorStream(actorProps, actor-name)
[3]Queue of RDDs as a Stream:
 For testing a Spark Streaming application with test data, one can also
create a  DStream based on a queue of RDDs, using streamingContext.queueStream(queueOfRDDs)
Each RDD pushed into the queue will be treated as a batch of data in the DStream, and processed like a stream.
 */

object RDDQueueStream {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkConf().setAppName("QueueStream").setMaster("local[3]")
    // Create the context
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    //创建RDD队列
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    // 创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue)

    //处理队列中的RDD数据
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    //打印结果
    reducedStream.print()

    //启动计算
    ssc.start()

    // Create and push some RDDs into
    for (i <- 1 to 10) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 3000, 10)


    }
     Thread.sleep(5000)
    //通过程序停止StreamingContext的运行
    ssc.stop()
  }
}
