package com.dt.spark.main.HighPerformenceSpark.ch5

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by hjw on 17/9/24.
  */
object MapPartionsIter2Iter {
  /**
    * This sub routine returns an Iterator of (columnIndex, value) that correspond
    * to one of the desired rank statistics on this partition.
    *
    * Because in the original iterator, the pairs are distinct
    * and include the count, one row of the original iterator could map to multiple
    * elements in the output.
    *
    *  i.e. if we were looking for the 2nd and 3rd element in column index 4 on
    * this partition. And the head of this partition is
    * ((3249.0, 4), 23)//这个是原始数聚合好的
    * (i.e. the element 3249.0 in the 4 th column appears 23 times),
    * then we would output (4, 3249.0) twice in the final iterator.
    * Once because 3249.0 is the 2nd element and once because it is the third
    * element on that partition for that column index and we are looking for both the
    * second and third element.
    *
    * @param valueColumnPairsIter passed in from the mapPartitions function.
    *                             An iterator of the sorted:
    *                             ((value, columnIndex), count) tupples.
    * @param targetsInThisPart - (columnIndex, index-on-partition pairs). In the above
    *                          example this would include (4, 2) and (4,3) since we
    *                          desire the 2nd element for column index 4 on this
    *                          partition and the 3rd element.
    * @return All of the rank statistics that live in this partition as an iterator
    *          of (columnIndex, value pairs)
    */

  //  MapPartitions example without an iterator-to-iterator transformation
  // [1] loop through the iterator,
  // [2] store the running totals in a hashMap,
  // [3] and build a new collection of the elements we want to keep using an array buffer
  // [4] then convert the array buffer to an iterator, shown later.

  def withArrayBuffer(valueColumnPairsIter : Iterator[((Double, Int), Long)], targetsInThisPart: List[(Int, Long)] ): Iterator[(Int, Double)] = {

    val columnsRelativeIndex: Predef.Map[Int, List[Long]] = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))


    // The column indices of the pairs that are desired rank statistics that live in
    // this partition.
    val columnsInThisPart: List[Int] = targetsInThisPart.map(_._1).distinct

    // A HashMap with the running totals of each column index. As we loop through
    // the iterator, we will update the hashmap as we see elements of each
    // column index.
    val runningTotals : HashMap[Int, Long]= new HashMap()
     runningTotals ++= columnsInThisPart.map(columnIndex => (columnIndex, 0L)).toMap

    //we use an array buffer to build the resulting iterator
    val result: ArrayBuffer[(Int, Double)] = new ArrayBuffer()

    valueColumnPairsIter.foreach {
      case ((value, colIndex), count) =>
        if (columnsInThisPart contains colIndex) {
          val total = runningTotals(colIndex)
          //the ranks that are contained by this element of the input iterator.
          //get by filtering the
          val ranksPresent = columnsRelativeIndex(colIndex)
            .filter(index => (index <= count + total) && (index > total))
          ranksPresent.foreach(r => result += ((colIndex, value)))
          //update the running totals.
          runningTotals.update(colIndex, total + count)
        } }

    //convert
    result.toIterator
  }

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
    val KVPairRDD = sc.parallelize(List(
      (1.0, 1), (4.0, 2),
      (1.0, 1), (5.0, 2),
      (1.0, 1), (6.0, 2),
      (2.0, 1), (7.0, 2),
      (3.0, 1), (8.0, 2)
    )
    )
    val mapRDD = KVPairRDD.map(iter => (iter,1L)).reduceByKey(_ + _).sortBy(_._1._1)
    mapRDD.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
    //    (1.0,1) : 3
    //    (2.0,1) : 1
    //    (3.0,1) : 1
    //    (4.0,2) : 1
    //    (5.0,2) : 1
    //    (6.0,2) : 1
    //    (7.0,2) : 1
    //    (8.0,2) : 1
    val iterRDD = mapRDD.toLocalIterator
    val iter = withArrayBuffer(iterRDD,List((1,1L),(1,2L),(1,4L),(1,5L)))
    for(it <- iter ){
      println(it._1 + "--" + it._2)
    }
    //    1--1.0
    //    1--1.0
    //    1--2.0
    //    1--3.0



    sc.stop()
   }

  }
