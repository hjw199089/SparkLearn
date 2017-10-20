package com.dt.spark.main.DataFrameToRDDLearn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


object DataFrameToRDD_2 {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)



    val sqlContext = new HiveContext(sc)
    //==========================================
    /*
    通过编程指定模式将DataFrame转化成RDD
     */
    val people = sc.textFile("./src/com/dt/spark/main/DataFrameToRDDLearn/srcFile/people.txt")

    //添加隐式转化,利用toDF
    //import sqlContext.implicits._

    //==========================================
    /*
    指定一个schema包含的列名
     */
    val schemaString = "name age"

    //==========================================
    /*
    构建schema
    //import org.apache.spark.sql.types.{StructField, StringType, StructType}
     */
     val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))//org.apache.spark.sql.types.StructType(schemaString.split(" ").map(fieldName=>(fieldName,StringType,true)))


    //==========================================
    /*
    将RDD转化成DataFrame的Row类型
    将RDD中的每条记录转换成一个行(Row)的实例
     */
    val rowRDD= people.map(_.split(",")).map(p=>Row(p(0),p(1).trim))

    val peopleDF = sqlContext.createDataFrame(rowRDD,schema)

    peopleDF.registerTempTable("peoletablefromschema")

    //==========================================
    /*
    查看当前库的全部表名
     */
    sqlContext.tableNames().foreach(println(_))
    //peoletablefromschema

    //==========================================
    /*
     利用sql查询
     */
    val teenagers = sqlContext.sql("select name from peoletablefromschema where age >= 13 and age <= 19")
    teenagers.map(t=>"Name: " + t(0)).collect().foreach(println)
    //    Name: Justin
    //    Name: Herry

    val dt = "20161208"
    val squaresDF = sc.makeRDD(s"""{"instid":"1","ctime":"1","event_type":"1","status":"1","event_id":"10014","path_info":"OK"}""".stripMargin:: Nil)
    println(squaresDF)
    val otherPeople = sqlContext.read.json(squaresDF)

    sc.stop()

  }

}
