package com.dt.spark.main.RDDLearn.RDDAggrFunAPI


import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjw on 16/12/06.
  */

//==========================================
/*
 [1]aggregate:将RDD元素由类型T聚合成U输出
 即给定类型U初识值zeroValue 利用Function2[U, T, U]将每个分区中元素聚合成U类型的输出,然后Function2[U, U, U]对分区聚合
 def aggregate[U](zeroValue : U)(seqOp : scala.Function2[U, T, U], combOp : scala.Function2[U, U, U])
 (implicit evidence$33 : scala.reflect.ClassTag[U]) : U = { /* compiled code */ }
 [2]reduce:在各个分区上将元素归并,最后在分区间归并
 def reduce(f : scala.Function2[T, T, T]) : T = { /* compiled code */ }
 [3]fold:与reduce类似只是提供一个初识值
 def reduce(f : scala.Function2[T, T, T]) : T = { /* compiled code */ }
 */

object RDDAggrFunAPI {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")


    val sc = new SparkContext(conf)

    //==========================================
    /*
     aggregate:将RDD元素由类型T聚合成U输出
     即给定类型U初识值zeroValue 利用Function2[U, T, U]将每个分区中元素聚合成U类型的输出,然后Function2[U, U, U]对分区聚合
     def aggregate[U](zeroValue : U)(seqOp : scala.Function2[U, T, U], combOp : scala.Function2[U, U, U])
     (implicit evidence$33 : scala.reflect.ClassTag[U]) : U = { /* compiled code */ }
     */
    val listRDDExample = sc.parallelize(List("I","Love","You"),2)

    val resList:List[String] = listRDDExample.aggregate[List[String]](Nil)((listInit,elem)=>elem::listInit,(listA,ListB)=>listA:::ListB)
    //List("I","You","Love")
    resList.foreach(println(_))
    //    I
    //    You
    //    Love

    //==========================================
    /*
     reduce:在各个分区上将元素归并,最后在分区间归并
     def reduce(f : scala.Function2[T, T, T]) : T = { /* compiled code */ }
     */
    val listRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    val sum  = listRDD.reduce((a,b)=> a+b )
    val sum2 = listRDD.sum()
    println("sum = " + sum)
    println("sum2 = " + sum2)
    //    sum = 6
    //    sum2 = 6.0

    val max = listRDD.reduce((a,b)=> if(a>b) a else b)
    val max2 = listRDD.reduce((a,b)=> Math.max(a,b))
    val max3 = listRDD.max()
    println("max = " + max)
    println("max2 = " + max2)
    println("max3 = " + max3)
    //    max = 3
    //    max2 = 3
    //    max3 = 3


    //==========================================
    /*
     fold:与reduce类似只是提供一个初识值
     def reduce(f : scala.Function2[T, T, T]) : T = { /* compiled code */ }
     */
    val sum3  = listRDD.fold(0)((a,b)=> a+b)
    println("分区数: " + listRDD.partitions.size)
    println("sum3 = " + sum3)
    //    分区数: 2
    //    sum3 = 6


    //这块待续
    val sum4  = listRDD.fold(1)((a,b)=> a+b)
    println("分区数: " + listRDD.partitions.size)
    println("sum4 = " + sum4)
    //    分区数: 2
    //    sum3 = 9


    sc.stop()


  }

}
