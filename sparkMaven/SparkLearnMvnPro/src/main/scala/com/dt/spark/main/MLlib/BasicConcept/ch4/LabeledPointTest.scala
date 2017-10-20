package com.dt.spark.main.MLlib.BasicConcept.ch4

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjw on 17/1/18.
  */

/*
一、向量标签作用：标识不同值
【1】生成方式：
1:直接静态生成向量标签
标记点
内容
2:文件API生成
loadLibSVMFile
格式:(标签,稀疏向量)
【2】注意事项：
1）索引要从1开始，从0开始的时候生成的内部索引时从-1开始，而且数据长度会比实际少1；
2）数据的长度是以最大列数为准的，因此最好是要保持数据列数一致；
3）标签列可以重复
【3】相关资料：
生成libSVM的数据格式及使用方法总结：点击打开链接
 */
object LabeledPointTest {

  def main(args: Array[String]) {
    //=======1:直接静态生成向量标签=======
    //密集型向量测试
    val vd:Vector = Vectors.dense(1,2,3)
    //建立标记点内容数据
    val pos = LabeledPoint(1,vd)
    //标记点和内容属性(静态类)
    println(pos.label + "\n" + pos.features)
    //       1.0
    //      [1.0,2.0,3.0]


    //=======2:文件API生成=======
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("LabeledPointLearn")

    val sc = new SparkContext(conf)
    val mu = MLUtils.loadLibSVMFile(sc,"./src/com/dt/spark/main/MLlib/BasicConcept/src/labeledPointTestData.txt")
    mu.foreach(println)

    //labeledPointTestData.txt
    //    1 1:2 2:3 3:4
    //    2 1:1 2:2 3:3
    //    1 1:1 2:3 3:3
    //    1 1:3 2:1 3:3
    //结果
    //    (1.0,(3,[0,1,2],[2.0,3.0,4.0]))
    //    (2.0,(3,[0,1,2],[1.0,2.0,3.0]))
    //    (1.0,(3,[0,1,2],[1.0,3.0,3.0]))
    //    (1.0,(3,[0,1,2],[3.0,1.0,3.0]))


  }
}
