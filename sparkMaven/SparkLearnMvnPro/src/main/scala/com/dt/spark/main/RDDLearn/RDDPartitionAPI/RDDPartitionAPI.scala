package com.dt.spark.main.RDDLearn.RDDPartitionAPI

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by  on 16/7/17.
  */

//==========================================
/*
[1]coalesce:对RDD重新分区
def coalesce(numPartitions : scala.Int, shuffle : scala.Boolean = { /* compiled code */ })(implicit ord : scala.Ordering[T]
(1)若减少分区,直接设置新的分区数即可
(2)若增加分区个数,设置shuffle = true

应用:
当将大数据集过滤处理后,分区中数据很小,可以减少分区数
当将小文件保存到外部存储系统时,将分区数设置为1, 将文件保存在一个文件中
当分区数小会造成CUP的浪费,适当增加分区

[2]repartition 和 coalesce 相似,只是将shuffle默认设置为true
def repartition(numPartitions : scala.Int)(implicit ord : scala.Ordering[T]
 */

object RDDPartitionAPI {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
//    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //==========================================
    /*
     构建RDD
     [1]从scala数据集构建RDD: parallelize()
     */
    val listRDDExample = sc.parallelize(List("1","2","3"),2)

    //==========================================
    /*
     获取分区数
     */
    val partitionsSzie = listRDDExample.partitions.size
    println(partitionsSzie)
    //2

    //==========================================
    /*
     [1]coalesce:对RDD重新分区
      def coalesce(numPartitions : scala.Int, shuffle : scala.Boolean = { /* compiled code */ })(implicit ord : scala.Ordering[T]
      (1)若减少分区,直接设置新的分区数即可
      (2)若增加分区个数,设置shuffle = true
     */
    val rePartitionsSzie  = listRDDExample.coalesce(1).partitions.size
    println(rePartitionsSzie)
    //1

    val rePartitionsSzie2  = listRDDExample.coalesce(3,true).partitions.size
    println(rePartitionsSzie2)
    //3
    sc.stop()

  }

}
