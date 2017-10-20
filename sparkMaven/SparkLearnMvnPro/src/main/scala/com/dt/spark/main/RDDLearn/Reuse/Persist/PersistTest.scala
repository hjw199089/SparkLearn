package com.dt.spark.main.RDDLearn.Reuse.Persist

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjw on 17/9/27.
  */
object PersistTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //==========================================
    /*
     构建RDD
     [1]从scala数据集构建RDD: parallelize()
     */
    val listRDDExample = sc.parallelize(List("4","1","6","7","5","9","10","0","8","2"),2)
    //10%采样后拉取数据至Driver
    val sortedRDD = listRDDExample.sortBy(f=>f)//触发一个job
    //sortedRDD.persist(StorageLevel.MEMORY_ONLY)
    val cnt = sortedRDD.count()
    val sample:Long = cnt/10
    sortedRDD.take(sample.toInt)
    while(true){
      ;
    }
    sc.stop()
  }
}
