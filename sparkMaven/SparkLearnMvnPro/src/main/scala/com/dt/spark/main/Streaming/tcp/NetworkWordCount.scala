package com.dt.spark.main.Streaming.tcp

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._ // not necessary since Spark 1.3
/**
  * Created by hjw on 17/4/17.
  */
/*
1. Define the input sources by creating input DStreams.
例如:
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

2. Define the streaming computations by applying transformation and output operations to DStreams.
例如:
    val lines = ssc.socketTextStream("localhost", 9999)
Basic Sources:
[1]File Streams: For reading data from files on any file system compatible with the HDFS API (that is, HDFS, S3, NFS, etc.), a DStream can be created as:
[2]Streams based on Custom Actors: DStreams can be created with data streams received through Akka actors by using streamingContext.actorStream(actorProps, actor-name)
[3]Queue of RDDs as a Stream

3. Start receiving data and processing it using streamingContext.start().
4. Wait for the processing to be stopped (manually or due to any error) using
streamingContext.awaitTermination().
5. The processing can be manually stopped using streamingContext.stop().

Points to remember:
Once a context has been started, no new streaming computations can be set up or added to it. Once a context has been stopped, it cannot be restarted.
Only one StreamingContext can be active in a JVM at the same time.
stop() on StreamingContext also stops the SparkContext. To stop only the StreamingContext, set the optional parameter of stop() called stopSparkContext to false.
A SparkContext can be re-used to create multiple StreamingContexts, as long as the previous StreamingContext is stopped (without stopping the SparkContext) before the next StreamingContext is created.


Points to remember
设置local [n]  > =  stream源数
When running a Spark Streaming program locally, do not use “local” or “local[1]” as
the master URL. Either of these means that only one thread will be used for running tasks locally.
 If you are using a input DStream based on a receiver (e.g. sockets, Kafka, Flume, etc.),
 then the single thread will be used to run the receiver, leaving no thread for processing the received data.
 Hence, when running locally, always use “local[n]” as the master URL, where n > number of receivers to run
*/
object NetworkWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit ={
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" ")) // not necessary since Spark 1.3 // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
