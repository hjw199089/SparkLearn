package com.dt.spark.main.MLlib.BasicConcept.ch4

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by hjw on 17/1/18.
  */
/*
1-基本数据类型:
Local vector   本地向量
Labeled point  向量标签
Local matrix   本地矩阵
Distributed matrix 分布式矩阵
2-数据格式:
整数和浮点型

1-1 本地向量
稀疏型:spares
  def sparse(size : scala.Int, indices : scala.Array[scala.Int], values : scala.Array[scala.Double])
  第一个参数:大于等于向量个数, 第二个参数:为values的index(可以跳过某些参数,但必须递增),第三个参数:元素值

  def sparse(size : scala.Int, elements : scala.Seq[scala.Tuple2[scala.Int, scala.Double]])

  def sparse(size : scala.Int, elements : java.lang.Iterable[scala.Tuple2[java.lang.Integer, java.lang.Double]])
密集型:dense
 */
object LocalVectorTest {
  def main(args: Array[String]) {
   //====dense
    val vd:Vector = Vectors.dense(1,2,3,4) // spark.mllib.linalg.{Vectors,Vector}包下的密集型向量
    println(vd(2))
    //    3.0
    //====sparse
    val vs:Vector = Vectors.sparse(5,Array(0,1,3,4),Array(1,2,3,4))
    println(vs(4))
    //    3.0
    val vs2:Vector = Vectors.sparse(5,Array(0,1,4,3),Array(1,2,3,4))
    println(vs2(4))
    //    0.0  必须按递增

    val vs3:Vector = Vectors.sparse(5,Array((1,1.0),(2,2.0)))
    println(vs3(1))
    //1.0
  }
}



