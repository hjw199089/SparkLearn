package com.dt.spark.main.RDDLearn.PairRDDAggrFunAPI


import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by  on 16/7/17.
  */

//==========================================
/*
  [1]aggregateByKey与aggregate相似,作用于PairRDD
    def aggregateByKey[U](zeroValue : U, partitioner : org.apache.spark.Partitioner)(seqOp : scala.Function2[U, V, U], combOp : scala.Function2[U, U, U]): org.apache.spark.rdd.RDD[scala.Tuple2[K, U]]
    def aggregateByKey[U](zeroValue : U, numPartitions : scala.Int)(seqOp : scala.Function2[U, V, U], combOp : scala.Function2[U, U, U]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, U]]
    def aggregateByKey[U](zeroValue : U)(seqOp : scala.Function2[U, V, U], combOp : scala.Function2[U, U, U]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, U]]
  [2]combineByKey,作用于PairRDD
    与aggregateByKey相比 ,提供一个函数将每个分区中首个元素转换成类型C,再聚合
    def combineByKey[C](createCombiner : scala.Function1[V, C], mergeValue : scala.Function2[C, V, C], mergeCombiners : scala.Function2[C, C, C], partitioner : org.apache.spark.Partitioner, mapSideCombine : scala.Boolean = { /* compiled code */ }, serializer : org.apache.spark.serializer.Serializer = { /* compiled code */ }) : org.apache.spark.rdd.RDD[scala.Tuple2[K, C]] = { /* compiled code */ }
    def combineByKey[C](createCombiner : scala.Function1[V, C], mergeValue : scala.Function2[C, V, C], mergeCombiners : scala.Function2[C, C, C], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, C]] = { /* compiled code */ }
  [3]reduceByKey每个分区中已(V,V)=>V,最终得到RDD(K,V)
    def reduceByKey(partitioner : org.apache.spark.Partitioner, func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
    def reduceByKey(func : scala.Function2[V, V, V], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
    def reduceByKey(func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
  [4]groupByKey:(K,V)--->(K,Iterable[v1,v2,....])
    def groupByKey() : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Iterable[V]]] = { /* compiled code */ }

 */
object PairRDDAggrFunAPI {
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
    val KVPairRDD = sc.parallelize(List(("中国", "山东"), ("中国", "北京"), ("中国", "上海"), ("美国", "洛杉矶"), ("美国", "德克萨斯"), ("韩国", "首尔")))


    //==========================================
    /*
    aggregateByKey与aggregate相似,作用于PairRDD
    def aggregateByKey[U](zeroValue : U, partitioner : org.apache.spark.Partitioner)(seqOp : scala.Function2[U, V, U], combOp : scala.Function2[U, U, U]): org.apache.spark.rdd.RDD[scala.Tuple2[K, U]]
    def aggregateByKey[U](zeroValue : U, numPartitions : scala.Int)(seqOp : scala.Function2[U, V, U], combOp : scala.Function2[U, U, U]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, U]]
    def aggregateByKey[U](zeroValue : U)(seqOp : scala.Function2[U, V, U], combOp : scala.Function2[U, U, U]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, U]]
    */
    val res = KVPairRDD.aggregateByKey[List[String]](Nil)(
      (listInit, value) => value :: listInit, //每个分区上的根据相同的K聚合
      (listA, listB) => listA ::: listB //分区间根据K二次聚合
    ).collect()

    println("聚合结果: ")
    res.foreach(print(_))
    //    聚合结果:
    //      (美国,List(德克萨斯, 洛杉矶))(中国,List(上海, 北京, 山东))(韩国,List(首尔))


    //==========================================
    /*
    combineByKey,作用于PairRDD
    与aggregateByKey相比 ,提供一个函数将每个分区中首个元素转换成类型C,再聚合
    def combineByKey[C](createCombiner : scala.Function1[V, C], mergeValue : scala.Function2[C, V, C], mergeCombiners : scala.Function2[C, C, C], partitioner : org.apache.spark.Partitioner, mapSideCombine : scala.Boolean = { /* compiled code */ }, serializer : org.apache.spark.serializer.Serializer = { /* compiled code */ }) : org.apache.spark.rdd.RDD[scala.Tuple2[K, C]] = { /* compiled code */ }
    def combineByKey[C](createCombiner : scala.Function1[V, C], mergeValue : scala.Function2[C, V, C], mergeCombiners : scala.Function2[C, C, C], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, C]] = { /* compiled code */ }
     */
    val res2 = KVPairRDD.combineByKey[List[String]](
      List(_:String),
      (list:List[String], v:String) => v :: list,
      (listA:List[String], listB:List[String]) => listA ::: listB
    ).collect()
    println("聚合结果: ")
    res2.foreach(print(_))
    //    聚合结果:
    //      (美国,List(德克萨斯, 洛杉矶))(中国,List(上海, 北京, 山东))(韩国,List(首尔))


    //==========================================
    /*
    与aggregateByKey相比 ,提供一个函数将每个分区中首个元素转换成类型C,再聚合,案例
    求每个key的平均值
    (K,V)---> (K,(和值,个数))-->求平均值
    设计createCombiner函数将V-->(V,1)
    元素将聚合(V+Vx,1+1)--->迭代下去
     */

    val KVPairRDD2 = sc.parallelize(List(("a", 1), ("a", 3), ("b", 3), ("b", 4)))
    val res3 = KVPairRDD2.combineByKey(
      (v) => (v, 1),
      (acc: (Int,Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._2,acc1._2+acc2._2)
    ).map{
      case(key,value)=>(key,value._1/value._2.toDouble)
    }
    println("\n每个key的平均值: ")
    res3.collect().foreach(println)
    //    (a,2.0)   (a,(1+3,1+1))--->(a,(1+3/(1+1),1+1))
    //    (b,3.5)


    //==========================================
    /*
    reduceByKey每个分区中已(V,V)=>V,最终得到RDD(K,V)
    def reduceByKey(partitioner : org.apache.spark.Partitioner, func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
    def reduceByKey(func : scala.Function2[V, V, V], numPartitions : scala.Int) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
    def reduceByKey(func : scala.Function2[V, V, V]) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
     */
    //求每个K的个数
    println("reduceByKey求每个K的和值: ")
    KVPairRDD2.reduceByKey(_+_).collect().foreach(println(_))
    //    (a,4)
    //    (b,7)


    //def Function2(a:String,b:String):String=(a,b)match{
    // case("",b)=>b
    // case(a,b)=>s"$a$b"
    // }
    val KVPairRDD3 = sc.parallelize(List((1, "I"), (1, "Love"), (2, "You"), (2, "!")))
    KVPairRDD3.reduceByKey{
      case("",b)=>b
      case(a,b)=>s"$a$b"
    }.collect().foreach(println(_))
    //    (1,ILove)
    //    (2,You!)

    //==========================================
    /*
    groupByKey:(K,V)--->(K,Iterable[v1,v2,....])
    def groupByKey() : org.apache.spark.rdd.RDD[scala.Tuple2[K, scala.Iterable[V]]] = { /* compiled code */ }
    */
    //TopN,实现每个K值的top2
    val KVPairRDD4 = sc.parallelize(List(("a", 1), ("a", 3), ("a", 4),("b", 3), ("b", 4),("b", 1), ("b", 4)))
    val groupRDD = KVPairRDD4.groupByKey().map(
      iter => {
        val key = iter._1
        val valuesIterable= iter._2
        val sortValues = valuesIterable.toList.sortWith(_>_).take(2)
        (key,sortValues)
      }
    )
    //将格式转换回去
    val flatgroupRDD = groupRDD.flatMap{
      case (key,sortValues)=>sortValues.map(key->_)
    }
    flatgroupRDD.foreach(println)
    //    (a,4)
    //    (a,3)
    //    (b,4)
    //    (b,4)

    sc.stop()
  }

}
