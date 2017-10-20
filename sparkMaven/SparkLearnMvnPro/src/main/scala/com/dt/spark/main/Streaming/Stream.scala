package com.dt.spark.main.Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by hjw on 16/12/25.
  */


object SQLContextSingleton{
  private var instance:SQLContext = null
  //lazy方式实例化
  def getInstance(sparkContext: SparkContext):SQLContext = synchronized{
    if(instance == null ){
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

case class Row(word : String)

object Stream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    val logFile= "./srcFile/"
    val conf = new SparkConf().setAppName("Stream").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    val words:DStream[String] = ssc.textFileStream(logFile)
    words.foreach(rdd =>
      if(!rdd.isEmpty()){
        val sQLContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sQLContext.implicits._
        val wordDataFrame = rdd.map(w => Row(w)).toDF()
        wordDataFrame.registerTempTable("words")
        val wordCountsDF = sQLContext.sql("select word,count(*) as total from group by word")
        wordDataFrame.show()
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
