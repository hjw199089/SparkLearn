package com.dt.spark.main.RDDLearn.PairRDDJoinSummer

/**
  * Created by hjw on 17/2/4.
  */

/*

待学习
http://www.cnblogs.com/zhenmingliu/archive/2011/12/29/2305775.html
http://blog.csdn.net/shulianghan/article/details/41011605

http://blog.csdn.net/hjw199089/article/details/54862966
 */
object PairRDDJoinSummer {

  //方法一:DF


  //方法二:PairRDD聚合函数


  //方法三:map-key的类聚合函数

//  val dimListMapping = sc.broadcast(dimList.map(a => {
//    ((a.srcPage, a.srcBlock, a.action), a)
//  }).collectAsMap())
//
//  val result = pbiLogEvent.map(a => ((a.getString(0), a.getString(1), a.getString(2)), a)).flatMap(a => {
//    var list = scala.collection.mutable.ArrayBuffer[(Int, String, String, Int, String, Int, String, String, String, Long, String, String, String, String, String)]()
//    val dimPbi = dimListMapping.value
//    val pbiDimEventConfig = dimPbi.get(a._1).get
//
//    val exposeExtraList: List[(String, String, String, String)] = parseExposeExtra(a._2.getString(12), pbiDimEventConfig)
//
//    exposeExtraList.foreach(e => {
//      list += ((pbiDimEventConfig.eventId, pbiDimEventConfig.eventName, a._2.getString(3), a._2.getInt(4), a._2.getString(5), a._2.getInt(6), groupDeviceType(a._2.getString(7)),
//        a._2.getString(8), groupUtmSource(a._2.getString(9)), a._2.getLong(10), e._1, e._2, e._3, e._4, a._2.getString(11)))
//    })
//
//    list.toList
//  })

}
