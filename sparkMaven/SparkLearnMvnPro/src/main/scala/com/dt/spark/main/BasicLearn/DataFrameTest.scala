package com.dt.spark.main.BasicLearn

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjw on 16/7/17.
  */
object DataFrameTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)


    val sqlContext = new SQLContext(sc)

    //create the DataFram
    val df = sqlContext.read.json("./srcFile/people.json")
    //    {"name":"Michael"}
    //    {"name":"Andy", "age":30}
    //    {"name":"Justin", "age":19}

    //show teh content of DataFram
    df.show()
    df.printSchema()
    //
    //    +----+-------+
    //    | age|   name|
    //    +----+-------+
    //    |null|Michael|
    //    |  30|   Andy|
    //    |  19| Justin|
    //    +----+-------+
    //
    //    root
    //    |-- age: long (nullable = true)
    //    |-- name: string (nullable = true)

    //select only "name" column
    df.select("name").show()
    //    +-------+
    //    |   name|
    //    +-------+
    //    |Michael|
    //    |   Andy|
    //    | Justin|
    //    +-------+


    //select everybody,but increment the age by 1
    //选择同时定义
    df.select(df("name"), df("age") + 1).show()
    //+-------+---------+
    //|   name|(age + 1)|
    //+-------+---------+
    //|Michael|     null|
    //|   Andy|       31|
    //| Justin|       20|
    //+-------+---------+


    //select people older than 21
    df.filter(df("age") > 21).show()
    //    +---+----+
    //    |age|name|
    //    +---+----+
    //    | 30|Andy|
    //    +---+----+
    //    count people by age
    df.groupBy("age").count().show()
    //    +----+-----+
    //    | age|count|
    //    +----+-----+
    //    |null|    1|
    //    |  19|    1|
    //    |  30|    1|
    //    +----+-----+


    sc.stop()

  }

}
