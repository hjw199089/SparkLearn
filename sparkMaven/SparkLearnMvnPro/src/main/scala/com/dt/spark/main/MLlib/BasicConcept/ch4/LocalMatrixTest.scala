package com.dt.spark.main.MLlib.BasicConcept.ch4

import org.apache.spark.mllib.linalg.Matrices

/**
  * Created by hjw on 17/1/19.
  */

//提高效率,矩阵运算
//Matrices.dense(行,列,Array(元素))
//备注:
//行列size必须和Array(元素)一致,缺多均throw error
object LocalMatrixTest {
  def main(args: Array[String]) {
    val mx = Matrices.dense(3,3,Array(1,1,1,2,2,2,3,3,3))
    println("本地矩阵元素: \n" + mx)
    //    本地矩阵元素:
    //    1.0  2.0  3.0
    //    1.0  2.0  3.0
    //    1.0  2.0  3.0
  }
}


