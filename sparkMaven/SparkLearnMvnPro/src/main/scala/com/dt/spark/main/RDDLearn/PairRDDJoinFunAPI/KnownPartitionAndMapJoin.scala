package com.dt.spark.main.RDDLearn.PairRDDJoinFunAPI

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * Created by hjw on 17/9/16.
  */

object KnownPartitionAndMapJoin {

  /*
  有数据
  (id,score)和(id,address)
  得到得分最高的地址
   */
  def joinScoresWithAddress1( scoreRDD : RDD[(Long, Double)],
                              addressRDD : RDD[(Long, String )]) : RDD[(Long, (Double, String))]= {
    val joinedRDD = scoreRDD.join(addressRDD)
    joinedRDD.reduceByKey( (x, y) => if(x._1 > y._1) x else y )
  }
  /*
  减少join的数据量,想reduceBykey
   */
  def joinScoresWithAddress2(scoreRDD : RDD[(Long, Double)],
                             addressRDD: RDD[(Long, String)]) : RDD[(Long, (Double, String))]= {
    val bestScoreData = scoreRDD.reduceByKey((x, y) => if(x > y) x else y)
    bestScoreData.join(addressRDD)
  }
  /*
  避免缺失,用leftOuterJoin
   */
  def outerJoinScoresWithAddress(scoreRDD : RDD[(Long, Double)],
                                 addressRDD: RDD[(Long, String)]) : RDD[(Long, (Double, Option[String]))]= {
    val joinedRDD = scoreRDD.leftOuterJoin(addressRDD)
    joinedRDD.reduceByKey( (x, y) => if(x._1 > y._1) x else y )
  }

  /*
  用known parttion 避免shuffle join
   */
  def joinScoresWithAddress3(scoreRDD: RDD[(Long, Double)],
                             addressRDD: RDD[(Long, String)])
  : RDD[(Long, (Double, String))]= {
    // If addressRDD has a known partitioner we should use that,
    // otherwise it has a default hash parttioner, which we can reconstruct by
    // getting the number of partitions.
    val addressDataPartitioner = addressRDD.partitioner match {
      case (Some(p)) => p
      case (None) => new HashPartitioner(addressRDD.partitions.length) }
    val bestScoreData = scoreRDD.reduceByKey(addressDataPartitioner, (x, y) => if(x > y) x else y)
    bestScoreData.join(addressRDD)
  }

  def manualBroadCastHashJoin[K : Ordering : ClassTag, V1 : ClassTag, V2 : ClassTag]
  (bigRDD : RDD[(K, V1)], smallRDD : RDD[(K, V2)])= {
    val smallRDDLocal: Map[K, V2] = smallRDD.collectAsMap()
    val smallRDDLocalBcast = bigRDD.sparkContext.broadcast(smallRDDLocal)
    bigRDD.mapPartitions(iter => {
      iter.flatMap{
        case (k,v1 ) =>
          smallRDDLocalBcast.value.get(k) match {
            case None => Seq.empty[(K, (V1, V2))]
            case Some(v2) => Seq((k, (v1, v2)))
          }
      }
    }, preservesPartitioning = true)
  }


  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val coreRDD = sc.parallelize(List((1L,1.0),(2L,3.0),(3L,90.0),(4L,100.0)),2)
    val addressRDD = sc.parallelize(List((1L,"Japan"),(2L,"USA"),(3L,"Indian"),(4L,"China")),2)
    val resRDD = manualBroadCastHashJoin(coreRDD,addressRDD)
    val res = resRDD.collect().apply(1)
    println(res._1 + "->(" + res._2._1 + " , " + res._2._2 + ")" )
    //4->(100.0 , China)

    while(true){;}
    sc.stop()
  }

}
