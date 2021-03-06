package com.dt.spark.main.Streaming.UpdateStateByKey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * Created by hjw on 17/4/28.
  *
  * updateStateByKey(func)
  * Return a new "state" DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values for the key. This can be used to maintain arbitrary state data for each key
  *1. Define the state - The state can be an arbitrary data type.
   2. Define the state update function - Specify with a function how to update the state using the previous state and the new values from an input stream.
  * Spark Streaming的解决方案是累加器，工作原理是，定义一个类似全局的可更新的变量，每个时间窗口内得到的统计值都累加(updated 更新 更为确切)到上个时间窗口得到的值，这样这个累加值就是横跨多个时间间隔
  */
object UpdateStateByKey {
  def main(args: Array[String]) {


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

    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[3]")
    // Create the context with a 5 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint(".")
    // Initial RDD input to updateStateByKey
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    // Create a ReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited test (eg. generated by 'nc')
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    // Update the cumulative count using updateStateByKey
    // This will give a Dstream made of state (which is the cumulative count of the words)
    //注意updateStateByKey的四个参数，第一个参数是状态更新函数
    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
