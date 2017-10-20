package com.dt.spark.main.RDDLearn.RDDCollectAsMap

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hjw on 17/2/4.
  */
/*
collect (较常用)
将RDD中的数据收集起来，变成一个Array，仅限数据量比较小的时候。

collectAsMap()
返回hashMap包含所有RDD中的分片，key如果重复，后边的元素会覆盖前面的元素。
/**
   * Return the key-value pairs in this RDD to the master as a Map.
   *
   * Warning: this doesn't return a multimap (so if you have multiple values to the same key, only
   *          one value per key is preserved in the map returned)
   *
   * @note this method should only be used if the resulting data is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def collectAsMap(): Map[K, V] = self.withScope {
    val data = self.collect()
    val map = new mutable.HashMap[K, V]
    map.sizeHint(data.length)
    data.foreach { pair => map.put(pair._1, pair._2) }
    map
  }
 */
object RDDCollectAsMapTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e"), (3, "g")) )
    val rddMap = rdd.collectAsMap()
    rddMap.foreach(println(_))
//    (2,e)
//    (1,c)
//    (3,g)
  }
}
