package com.dt.spark.main.RDDLearn.RDDzipWithIndexAPI

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by  on 16/7/17.
  */

//==========================================
/*
http://lxw1234.com/archives/2015/07/352.htm
 */

object RDDSortAPI {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)


    //==========================================
    /*
    1.sortByKey
    无可非议sortByKey是Spark的最常用的排序，简单的案例暂且跳过，下面给一个非简单的案例，进入排序之旅
    对下面简单元祖，要求先按元素1升序，若元素1相同，则再按元素3升序
     (1, 6, 3), (2, 3, 3), (1, 1, 2), (1, 3, 5), (2, 1, 2)
    提示：sortByKey对于key是单个元素排序很简单，如果key是元组如(X1，X2，X3.....)，它会先按照X1排序，若X1相同，则在根据X2排序，依次类推...
     */
    val array = Array((1, 1, List((1,2,List("A","B"))), (2, 2, List(1,2,List("A","B"))), (3, 3, List(1,2,List("A","B"))), (4, 4, List(1,2,List("A","B"))), (5, 5, List(1,2,List("A","B")))))
    val rdd1 = sc.parallelize(array,2)
//    rdd1.zipWithIndex().foreach(println(_))
////    ((1,6,3),0)
////    ((2,3,3),1)
////    ((1,1,2),2)
////    ((1,3,5),3)
////    ((2,1,2),4)

//    rdd1.map(a=>{
//      (a._1,a._2,a._3.zipWithIndex)
//    }).foreach(println _)
////    (1,1,List((1,0), (2,1), (3,2)))
////    (2,2,List((1,0), (2,1), (3,2)))
////    (3,3,List((1,0), (2,1), (3,2)))
////    (4,4,List((1,0), (2,1), (3,2)))
////    (3,3,List((1,0), (2,1), (3,2)))
////    (4,4,List((1,0), (2,1), (3,2)))
////    (5,5,List((1,0), (2,1), (3,2)))

    rdd1.map(a=>{
      (a._1,a._2,a._3.zipWithIndex)
    }).zipWithIndex().foreach(println(_))
    //    ((1,1,List((1,0), (2,1), (3,2))),0)
    //    ((2,2,List((1,0), (2,1), (3,2))),1)
    //    ((3,3,List((1,0), (2,1), (3,2))),2)
    //    ((4,4,List((1,0), (2,1), (3,2))),3)
    //    ((5,5,List((1,0), (2,1), (3,2))),4)


    //(1,1,List((1,1,List((A,0), (B,1))), (2,2,List((C,0), (D,1)))))
    val array2 = Array((1, 1, List((1,1,List("A","B")),(2,2,List("C","D")))))
    val rdd2 = sc.parallelize(array2,2)
    rdd2.map(a=>{
      (a._1,a._2,a._3.map(a=>{
        (a._1,a._2,a._3.zipWithIndex)
      }).zipWithIndex
        )
    }).foreach(println _)

    sc.stop()

  }

}
//zipWithIndex
//
//def zipWithIndex(): RDD[(T, Long)]
//
//该函数将RDD中的元素和这个元素在RDD中的ID（索引号）组合成键/值对。
//
//scala> var rdd2 = sc.makeRDD(Seq("A","B","R","D","F"),2)
//rdd2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[34] at makeRDD at :21
//
//scala> rdd2.zipWithIndex().collect
//res27: Array[(String, Long)] = Array((A,0), (B,1), (R,2), (D,3), (F,4))
//
//zipWithUniqueId
//
//def zipWithUniqueId(): RDD[(T, Long)]
//
//该函数将RDD中元素和一个唯一ID组合成键/值对，该唯一ID生成算法如下：
//
//每个分区中第一个元素的唯一ID值为：该分区索引号，
//
//每个分区中第N个元素的唯一ID值为：(前一个元素的唯一ID值) + (该RDD总的分区数)
//
//看下面的例子：
//
//scala> var rdd1 = sc.makeRDD(Seq("A","B","C","D","E","F"),2)
//rdd1: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[44] at makeRDD at :21
////rdd1有两个分区，
//scala> rdd1.zipWithUniqueId().collect
//res32: Array[(String, Long)] = Array((A,0), (B,2), (C,4), (D,1), (E,3), (F,5))
////总分区数为2
////第一个分区第一个元素ID为0，第二个分区第一个元素ID为1
////第一个分区第二个元素ID为0+2=2，第一个分区第三个元素ID为2+2=4
////第二个分区第二个元素ID为1+2=3，第二个分区第三个元素ID为3+2=5
