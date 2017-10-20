package com.dt.spark.main.DataFrameToRDDLearn

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by  on 16/7/17.
  */

//定义样本类,此时列名和类型已知
case class People(name:String, age:Int)
// case class Out(instid:String, ctime:String,event_type:String,status:String,event_id:String,path_info:String)
case class Out(instid:String)

object DataFrameToRDD_1 {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val sql = "insert into  hubble_event_instance_path (inst_id,ctime,event_type,status,event_id,path_info) values(" + 1 + "," + 1 +"," + 1 +"," + "2"+"," + 1  + ","+ "''" + ")"

    println(sql)
    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val calender = Calendar.getInstance()
     calender.add(Calendar.DATE, 0)
    val ctime = sdf.format(calender.getTime)
    println(ctime.toString)

    val sqlContext = new HiveContext(sc)
    //==========================================
    /*
    通过反射将DataFrame转化成RDD
     */
    val people = sc.textFile("./src/com/dt/spark/main/DataFrameToRDDLearn/srcFile//people.txt")

    //添加隐式转化,利用toDFevent_id
    import sqlContext.implicits._


//    val listRDDExample = sc.parallelize(List("11111","22222","st","ab","10014","OK"),2).map(p=>Out(p.getString(0)))
//
//    val outDF = listRDDExample.toDF("instid","ctime","event_type","status","event_id","path_info")
//    outDF.show()
    val dt = "20161208"
     val squaresDF = sc.makeRDD(s"""{"instid":"1","ctime":"$dt","event_type":"1","status":"1","event_id":"10014","path_info":"OK"}""".stripMargin:: Nil)
     println(squaresDF)
     val otherPeople = sqlContext.read.json(squaresDF)



     otherPeople.show()


    val peopleDF = people.map(_.split(",")).map(p=>People(p(0),p(1).trim.toInt)).toDF()

    //peopleDF.show()
    //    +-------+---+
    //    |   name|age|
    //    +-------+---+
    //    |Michael| 29|
    //    |   Andy| 30|
    //    | Justin| 19|
    //    |   john| 20|
    //    |  Herry| 19|
    //    +-------+---+

    val peopleRDD2 = sc.makeRDD(s"""{"na":"Michael","city":"sd"}""".stripMargin:: Nil)
    val peopleRDD2DF = sqlContext.read.json(peopleRDD2)
    peopleDF.join(peopleRDD2DF,peopleDF("name") === peopleRDD2DF("na"),"inner").show()


    //==========================================
    /*
     查看DataFrame和RDD间的关系
     */
    println(peopleDF.rdd.toDebugString)

    //    (1) MapPartitionsRDD[6] at rdd at DataFrameToRDD_1.scala:56 []
    //    |  MapPartitionsRDD[4] at rddToDataFrameHolder at DataFrameToRDD_1.scala:39 []
    //    |  MapPartitionsRDD[3] at map at DataFrameToRDD_1.scala:39 []
    //    |  MapPartitionsRDD[2] at map at DataFrameToRDD_1.scala:39 []
    //    |  MapPartitionsRDD[1] at textFile at DataFrameToRDD_1.scala:34 []
    //    |  ./src/com/dt/spark/main/DataFrameToRDDLearn/srcFile//people.txt HadoopRDD[0] at textFile at DataFrameToRDD_1.scala:34 []


    //==========================================
    /*
    注册临时表,执行SQL查询
     */

    peopleDF.registerTempTable("peopletable")

    //==========================================
    /*
    查看当前库的信息
     */

    sqlContext.sql("show databases").show()
    //    +-------+
    //    | result|
    //    +-------+
    //    |default|
    //    +-------+

    //==========================================
    /*
    查看当前库的全部表名
     */
    sqlContext.tableNames().foreach(println(_))
    // peopletable

    //==========================================
    /*
     利用sql查询
     */
     val teenagers = sqlContext.sql("select name from peopletable where age >= 13 and age <= 19")
    teenagers.map(t=>"Name: " + t(0)).collect().foreach(println)




    sc.stop()

  }

}
