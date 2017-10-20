/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
/**
  * Created by hjw on 17/5/7.
  */

package com.dt.spark.main.Streaming.Kafka

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object DirectKafkaWordCount {

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

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"auto.offset.reset"->"smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // updateStateByKey 计算包含"WWW"单次的出现次数
    ssc.checkpoint("DirectKafkaWordCount-checkpoint")
    // Initial RDD input to updateStateByKey
    val initialRDD = ssc.sparkContext.parallelize(List(("WWW", 0)))
    val WWWwords = lines.filter(_.contains("WWW")).map(x=>("WWW",1))
    val stateDstream = WWWwords.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    stateDstream.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
