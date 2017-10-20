package com.dt.spark.main.MLlib.BasicConcept.ch4

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjw on 17/1/19.
  */
//分布式矩阵:当数据量较大时使用,行和列为Long,值为Double
//按照存储形式:
//(1) 行矩阵
//(2) 索引行矩阵
//(3) 坐标矩阵
//(4) 块矩阵
//1-行矩阵
//相同格式的特征向量的集合,可以行为单位读写
//行和列
//2-索引行矩阵
//行矩阵不便于调试显示,引入索引行矩阵
//类型转换为:行距阵,坐标矩阵,块矩阵
//3-坐标矩阵
//(x,y,value:Double)

object DistrubutedMatrixTest {

  def main(args: Array[String]) {
    //==========1-行矩阵================
    //相同格式的特征向量的集合,可以行为单位读写
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DistrubutedMatrixTest")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("./src/com/dt/spark/main/MLlib/BasicConcept/src/rowMatrix.txt")
    //rdd :MapPartitionsRDD[1]
    val rddtmp = rdd.map(_.split(" ").map(_.toDouble)) //切分并转成Double
      .map(Vectors.dense(_)) //每行转成航矩阵
    println(rddtmp)
    //MapPartitionsRDD

    val rm = new RowMatrix(rddtmp)
    println("行数: "+ rm.numRows() + "\n" +
      "列数: " + rm.numCols()
    )
    //    行数: 2
    //    列数: 4

    //==========2-索引行矩阵================
     val iRdd = rdd.map(_.split(" ").map(_.toDouble)) //切分并转成Double
      .map(Vectors.dense(_)) //每行转成航矩阵
      .map(vd => new IndexedRow(vd.size,vd)) //格式转化

    val indexRm = new IndexedRowMatrix(iRdd)
    //调试显示内容
    println("索引行矩阵行遍历:\n" )
    indexRm.rows.foreach(println(_))
    //    索引行矩阵行遍历:
    //      IndexedRow(4,[1.0,1.0,1.0,1.0])
    //      IndexedRow(4,[2.0,2.0,2.0,2.0])

    val toRm  = indexRm.toRowMatrix()
    val toCrm = indexRm.toCoordinateMatrix()
    val toBrm = indexRm.toBlockMatrix()


    //========3-坐标矩阵==========

    val rdd_1 = rdd.map(_.split(" ").map(_.toDouble))
      .map(value=>(value(0).toLong,value(1).toLong,value(2)))
      .map(va=>new MatrixEntry(va._1,va._2,va._3))


    val coorRm = new CoordinateMatrix(rdd_1)
    println(coorRm.entries.foreach(println))
    //    MatrixEntry(1,1,1.0)
    //    MatrixEntry(2,2,2.0)
    //    ()


    sc.stop()
  }

}
