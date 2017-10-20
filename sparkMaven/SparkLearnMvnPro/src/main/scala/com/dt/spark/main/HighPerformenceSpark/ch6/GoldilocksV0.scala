package com.dt.spark.main.HighPerformenceSpark.ch6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hjw on 17/9/30.
  */
object GoldilocksV0 {
  //每列不依赖其他的列,可以设法并行计算
  //将数据转化成一个List[K-V],K表示column对每个key并行计算
  def findRandStatics(
                       dataFram: DataFrame,
                       ranks: List[Long]
                     ): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    val numOfColumns = dataFram.schema.length
    var i = 0
    var resultMap  = Map[Int,Iterable[Double]]()
    //对每列排序打标过滤出目标记录
    while(i < numOfColumns){
      //取出第列
      val col = dataFram.rdd.map(row=>row.getDouble(i))
      val sortedColWithIdex:RDD[(Double,Long)] = col.sortBy(v=>v).zipWithIndex()
      //过滤出目标记录
      val ranksOnly = sortedColWithIdex.filter{
        //zipWithIndex的序号是从0开始的
        case (colVaule,index)=>ranks.contains(index + 1)
      }.keys
      val list = ranksOnly.collect()
      resultMap += (i+1 ->list)
      i += 1
    }
    resultMap
  }


  //Goldilocks version 1, mapping to column index/value pairs
  def mapToKeyValuePairs(dataFram: DataFrame):RDD[(Int,Double)] = {
    val rowLength =  dataFram.schema.length
    dataFram.rdd.flatMap(
      row => Range(0,rowLength).map(i=>(i,row.getDouble(i)))
    )
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sqlContext = new HiveContext(sc)
    //加载json文件
    val table = sqlContext.read.json("./src/main/scala/com/dt/spark/main/HighPerformenceSpark/srcFile/GoldilocksData.json").
      select("Happiness", "Niceness", "Softness", "Sweetness")

    //table.show()
    //    +---------+--------+---------+--------+---------+
    //    |Happiness|Niceness|Pandaname|Softness|Sweetness|
    //    +---------+--------+---------+--------+---------+
    //    |     15.0|    0.25|     Mama|  2467.0|      0.0|
    //    |      2.0|  1000.0|     Papa|    35.4|      0.0|
    //    |     10.0|     2.0|     Baby|    50.0|      0.0|
    //    |      3.0|     8.5|     Cacy|     0.2|     98.0|
    //    +---------+--------+---------+--------+---------+
    val res = findRandStatics(table,List(2,4))
    res.foreach(println(_))
    //    (1,WrappedArray(3.0, 15.0))
    //    (2,WrappedArray(2.0, 1000.0))
    //    (3,WrappedArray(35.4, 2467.0))
    //    (4,WrappedArray(0.0, 98.0))
    sc.stop()

  }
}
