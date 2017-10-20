package com.dt.spark.main.RDDLearn.PairRDDJoinFunAPI

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by  on 16/12/06.
  */

//==========================================
/*
  PairRDD间关联API,注意返回值类型
  def join[W](other :Tuple2[K, W]) : Tuple2[K, Tuple2[V, W]]
  def join[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[V, W]]] = { /* compiled code */ }

  def leftOuterJoin[W](other :Tuple2[K, W]) : Tuple2[K, Tuple2[V, Option[W]]
  def leftOuterJoin[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[V, scala.Option[W]]]] = { /* compiled code */ }

  def rightOuterJoin[W](other : Tuple2[K, W]) : Tuple2[K, Tuple2[Option[V], W]]
  def rightOuterJoin[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[scala.Option[V], W]]] = { /* compiled code */ }

  def fullOuterJoin[W](other : Tuple2[K, W]) : Tuple2[K, scala.Tuple2[Option[V], Option[W]]
  def fullOuterJoin[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[scala.Option[V], scala.Option[W]]]] = { /* compiled code */ }

 */
object PairRDDJoinFunAPI {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")


    val sc = new SparkContext(conf)

    //==========================================
    /*
  def join[W](other :Tuple2[K, W]) : Tuple2[K, Tuple2[V, W]]
  def join[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[V, W]]] = { /* compiled code */ }

  def leftOuterJoin[W](other :Tuple2[K, W]) : Tuple2[K, Tuple2[V, Option[W]]
  def leftOuterJoin[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[V, scala.Option[W]]]] = { /* compiled code */ }

  def rightOuterJoin[W](other : Tuple2[K, W]) : Tuple2[K, Tuple2[Option[V], W]]
  def rightOuterJoin[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[scala.Option[V], W]]] = { /* compiled code */ }

  def fullOuterJoin[W](other : Tuple2[K, W]) : Tuple2[K, scala.Tuple2[Option[V], Option[W]]
  def fullOuterJoin[W](other : org.apache.spark.rdd.RDD[scala.Tuple2[K, W]], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Tuple2[scala.Option[V], scala.Option[W]]]] = { /* compiled code */ }
    */
    val KVPairRDD1 = sc.parallelize(List(("beijing", "京"), ("shaanxi", "陕"),("jiangsu", "苏"),("shandong", "鲁"),("guangxi","桂")))
    val KVPairRDD2 = sc.parallelize(List( ("beijing", "北京"), ("shaanxi", "西安"),("jiangsu", "南京"),("shandong", "济南")))

    def getVauleOfOption(in:Option[String]):String = in match {
      case Some(x) => x
      case None => "NULL"
    }

    val res = KVPairRDD1.fullOuterJoin(KVPairRDD2).map(a=>(a._1,getVauleOfOption(a._2._1),getVauleOfOption(a._2._2)))
    res.foreach(println(_))
    //    (guangxi,桂,NULL)
    //    (shaanxi,陕,西安)
    //    (shandong,鲁,济南)
    //    (beijing,京,北京)
    //    (jiangsu,苏,南京)

    sc.stop()
  }

}
