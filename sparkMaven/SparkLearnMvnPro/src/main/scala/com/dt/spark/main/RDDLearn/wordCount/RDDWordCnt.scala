
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by  on 16/7/17.
  */

//==========================================
/*
 构建RDD
 [1]读外部文件: textFile()
 [2]从scala数据集构建RDD: parallelize()
 */

object RDDWordCnt {
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
    val listRDDExample = sc.parallelize(List("1","2","3"),2)

    //==========================================
    /*
     获取分区数
     */
    val partitionsSzie = listRDDExample.partitions.size
    println(partitionsSzie)
    //2



    //==========================================
    /*
     构建RDD
     [2]读外部文件: textFile()
     */
    val txtFile = sc.textFile("./src/main/scala/com/dt/spark/main/RDDLearn/wordCount/srcFile/readme.txt")

    //==========================================
    /*
     take(N)返回第N个元素,这里是返回第N行,注意返回类型:数组Array[T]
     */

    txtFile.take(1).foreach(println(_))
    // I love you

    //==========================================
    /*
     first返回第1个元素,这里是返回第1行,注意返回类型:String
     */
    txtFile.first().foreach(println(_))

    //==========================================
    /*
     count()计算元素个数
     */
    println(txtFile.count())
    //4

    //==========================================
    /*
     获得单次个数最多行的单次
     map对每个元素按照给定的function操作,
     */
    val max = txtFile.map(line=>line.split(" ").size).reduce((a,b)=>if(a>b) a else b)
    println(max)
    //8

    val max2 = txtFile.map(line=>line.split(" ").size).reduce((a,b)=>Math.max(a,b))
    println(max2)
    //8

    //==========================================
    /*
    单词计数
     */
    val words = txtFile.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
    //    find : 1
    //    Please : 1
    //    you : 2
    //    love : 1
    //    will : 1
    //    I : 2
    //    to : 1
    //      : 1
    //    best : 1
    //    try : 1
    //    for : 1
    //    my : 1
    //    waiting : 1
    //    me : 1
    sc.stop()


  }

}
