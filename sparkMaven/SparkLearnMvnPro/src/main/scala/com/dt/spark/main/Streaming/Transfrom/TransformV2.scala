package com.dt.spark.main.Streaming.Transfrom

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by hjw on 17/9/19.
  * 过滤hello,统计其他单次的出现的次数
  */
object TransformV2 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  ///函数常量定义，返回类型是Some(Int)，表示的含义是最新状态
  ///函数的功能是将当前时间间隔内产生的Key的value集合，加到上一个状态中，得到最新状态
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  ///入参是三元组遍历器，三个元组分别表示Key、当前时间间隔内产生的对应于Key的Value集合、上一个时间点的状态
  ///newUpdateFunc的返回值要求是iterator[(String,Int)]类型的
  val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    ///对每个Key调用updateFunc函数(入参是当前时间间隔内产生的对应于Key的Value集合、上一个时间点的状态）得到最新状态
    ///然后将最新状态映射为Key和最新状态
    iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
  }


  def main(args: Array[String]): Unit ={
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint(".")
    // Initial RDD input to updateStateByKey
    val initialRDD = ssc.sparkContext.parallelize(List(("world", 0)))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" ")) // not necessary since Spark 1.3 // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    val cleanedDStream = wordCounts.transform(rdd=>{
      rdd.filter(a=> !a._1.equals("hello"))
    })

    // Update the cumulative count using updateStateByKey
    // This will give a Dstream made of state (which is the cumulative count of the words)
    //注意updateStateByKey的四个参数，第一个参数是状态更新函数
    val stateDstream = cleanedDStream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    stateDstream.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

