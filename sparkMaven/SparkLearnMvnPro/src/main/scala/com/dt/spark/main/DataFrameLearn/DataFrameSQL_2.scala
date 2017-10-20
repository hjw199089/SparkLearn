package com.dt.spark.main.DataFrameLearn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
/**
  * spark-DataFrame学习记录-[2]解决spark-dataframe的JOIN操作之后产生重复列（Reference '***' is ambiguous问题解决）
  */
object DataFrameSQL_2 {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._


    var df = sc.parallelize(Array(
      ("one", "A", 1), ("one", "B", 2), ("two", "A", 3), ("two", "B", 4)
    )).toDF("key1", "key2", "value")
    df.show()
    //    +----+----+-----+
    //    |key1|key2|value|
    //    +----+----+-----+
    //    | one|   A|    1|
    //    | one|   B|    2|
    //    | two|   A|    3|
    //    | two|   B|    4|
    //    +----+----+-----+

    val df2 = sc.parallelize(Array(
      ("one", "A", 5), ("two", "A", 6)
    )).toDF("key1", "key2", "value2")
    df2.show()

    //    +----+----+------+
    //    |key1|key2|value2|
    //    +----+----+------+
    //    | one|   A|     5|
    //    | two|   A|     6|
    //    +----+----+------+

    println("==========")
    df = df.unionAll(df2)
    df.show()
    println("==========")

    val joined = df.join(df2, df("key1") === df2("key1") && df("key2") === df2("key2"), "left_outer")
    joined.show()

    //    +----+----+-----+----+----+------+
    //    |key1|key2|value|key1|key2|value2|
    //    +----+----+-----+----+----+------+
    //    | two|   A|    3| two|   A|     6|
    //    | two|   B|    4|null|null|  null|
    //    | one|   A|    1| one|   A|     5|
    //    | one|   B|    2|null|null|  null|
    //    +----+----+-----+----+----+------+

    df.join(df2, Seq("key1", "key2"), "left_outer").show()

    //    +----+----+-----+------+
    //    |key1|key2|value|value2|
    //    +----+----+-----+------+
    //    | two|   A|    3|     6|
    //    | two|   B|    4|  null|
    //    | one|   A|    1|     5|
    //    | one|   B|    2|  null|
    //    +----+----+-----+------+


    df.join(df2, Seq("key1")).show()
    //df
    //    +----+----+-----+
    //    |key1|key2|value|
    //    +----+----+-----+
    //    | one|   A|    1|
    //    | one|   B|    2|
    //    | two|   A|    3|
    //    | two|   B|    4|
    //    +----+----+-----+

    //df2
    //    +----+----+------+
    //    |key1|key2|value2|
    //    +----+----+------+
    //    | one|   A|     5|
    //    | two|   A|     6|
    //    +----+----+------+

    //    +----+----+-----+----+------+
    //    |key1|key2|value|key2|value2|
    //    +----+----+-----+----+------+
    //    | two|   A|    3|   A|     6|
    //    | two|   B|    4|   A|     6|
    //    | one|   A|    1|   A|     5|
    //    | one|   B|    2|   A|     5|
    //    +----+----+-----+----+------+


    val df22 = df2.withColumnRenamed("key1","k1").withColumnRenamed("key2","k2")

    df.join(df22,df("key1") === df22("k1") && df("key2") === df22("k2"), "left_outer").show()
    //    +----+----+-----+----+----+------+
    //    |key1|key2|value|  k1|  k2|value2|
    //    +----+----+-----+----+----+------+
    //    | two|   A|    3| two|   A|     6|
    //    | two|   B|    4|null|null|  null|
    //    | one|   A|    1| one|   A|     5|
    //    | one|   B|    2|null|null|  null|
    //    +----+----+-----+----+----+------+
    (df.join(df22,df("key1") === df22("k1") && df("key2") === df22("k2"), "left_outer").columns.foreach(println(_)))

    sc.stop()

  }

}
