package com.dt.spark.main.DataFrameLearn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
//
//例子描述：
//有个网站访问日志，有4个字段：（用户id，用户名，访问次数，访问网站）
//需要统计：
//1.用户的访问总次数去重
//2.用户一共访问了多少种不同的网站
//这里用sql很好写
//select id,name,count(distinct url) from table group by id,name
//其实这个题目是继官方和各种地方讲解聚合函数（aggregate）的第二个例子，第一个例子是使用aggregate来求平均数。
//我们先用简易版来做一遍，后续我更新一份聚合函数版
//
//原始数据：
//id1,user1,2,http://www.baidu.com
//id1,user1,2,http://www.baidu.com
//id1,user1,3,http://www.baidu.com
//id1,user1,100,http://www.baidu.com
//id2,user2,2,http://www.baidu.com
//id2,user2,1,http://www.baidu.com
//id2,user2,50,http://www.baidu.com
//id2,user2,2,http://www.sina.com
//
//结果数据：
//((id1,user1),4,1)
//((id2,user2),4,2)
//
//代码片段：
//[java] view plain copy 在CODE上查看代码片派生到我的代码片
//val sparkConf = new SparkConf().setAppName("DisFie").setMaster("local")
//val sc = new SparkContext(sparkConf)
//
//
//val source = Source.fromFile("C:\\10.txt").getLines.toArray
//val RDD0 = sc.parallelize(source)
//
//RDD0.map {
//lines =>
//val line = lines.split(",")
//((line(0), line(1)), (1, line(3)))
//}.groupByKey().map {
//case (x, y) =>
//val(n,url) = y.unzip
//(x,n.size,url.toSet.size)
//}.foreach(println)


//http://spark.apache.org/docs/1.3.1/api/scala/index.html#org.apache.spark.sql.DataFrame

/**
  * Created by hjw on 16/7/17.
  */
object DataFrameSQL {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sqlContext = new HiveContext(sc)
    //加载json文件
    val people = sqlContext.read.json("./src/com/dt/spark/main/DataFrameLearn/srcFile/people.json")
    val newpeople = sqlContext.read.json("./src/com/dt/spark/main/DataFrameLearn/srcFile/newpeople.json")
    val dept = sqlContext.read.json("./src/com/dt/spark/main/DataFrameLearn/srcFile/department.json")

    //==========================================
    /*
    表格形式显示DataFrame信息
    show():
            默认输出前20条
    show(n):
            可以设置参数
     */

    people.show()
    //    +---+-----+------+----------+-------+-------+
    //    |age|depId|gender|job number|   name|sarlary|
    //      +---+-----+------+----------+-------+-------+
    //    | 33|    1|  male|       001|Michael|   3000|
    //    | 30|    2|female|       002|   andy|   4000|
    //    | 19|    1|  male|       003| Justin|   4000|
    //    | 32|    1|  male|       004|   John|   4000|
    //    | 20|    2|female|       005|  Herry|   4000|
    //    | 26|    3|  male|       006|   Jack|   4000|
    //    +---+-----+------+----------+-------+-------+

    /*
      打印printSchema
    */
    people.printSchema()
    //    root
    //    |-- age: long (nullable = true)
    //    |-- depId: long (nullable = true)
    //    |-- gender: string (nullable = true)
    //    |-- job number: string (nullable = true)
    //    |-- name: string (nullable = true)
    //    |-- sarlary: long (nullable = true)

    //==========================================
    /*
    DataFrame基本信息的查询
    columns:已数组的形式返回列名字
    count:  返回总记录数
    take:   已数组的形式返回前n记录信息
    toJSON: 转换为JsonRDD
     */

    // people.columns
    people.columns.foreach(println(_))
    //    age
    //    depId
    //    gender
    //    job number
    //    name
    //    salary

    println(people.count())
    //6

    people.take(2).foreach(print(_))
    //[33,1,male,001,Michael,3000][30,2,female,002,andy,4000]

    people.toJSON.collect().foreach(println(_))

    //    {"age":33,"depId":1,"gender":"male","job number":"001","name":"Michael","salary":3000}
    //    {"age":30,"depId":2,"gender":"female","job number":"002","name":"andy","salary":4000}
    //    {"age":19,"depId":1,"gender":"male","job number":"003","name":"Justin","salary":4000}
    //    {"age":32,"depId":1,"gender":"male","job number":"004","name":"John","salary":4000}
    //    {"age":20,"depId":2,"gender":"female","job number":"005","name":"Herry","salary":4000}
    //    {"age":26,"depId":3,"gender":"male","job number":"006","name":"Jack","salary":4000}

   //==========================================
    /*
     DataFrame的条件查询
     filter:
     where:
    */

    println(people.filter("gender = 'male'").count())
    //4
    println(people.filter("age > 25").count())
    //4

    println(people.filter("age > 25").filter("gender = 'male'").count())
    // 3

    println(people.where("age > 25").count())
    //4

    //==========================================
    /*
    排序和分区排序
     */
    people.sort(people("age").asc).show

    //    +---+-----+------+----------+-------+------+
    //    |age|depId|gender|job number|   name|salary|
    //      +---+-----+------+----------+-------+------+
    //    | 19|    1|  male|       003| Justin|  4000|
    //    | 20|    2|female|       005|  Herry|  4000|
    //    | 26|    3|  male|       006|   Jack|  4000|
    //    | 30|    2|female|       002|   andy|  4000|
    //    | 32|    1|  male|       004|   John|  4000|
    //    | 33|    1|  male|       001|Michael|  3000|
    //    +---+-----+------+----------+-------+------+

    people.sortWithinPartitions("gender","age").show()
    //    +---+-----+------+----------+-------+------+
    //    |age|depId|gender|job number|   name|salary|
    //      +---+-----+------+----------+-------+------+
    //    | 20|    2|female|       005|  Herry|  4000|
    //    | 30|    2|female|       002|   andy|  4000|
    //    | 19|    1|  male|       003| Justin|  4000|
    //    | 26|    3|  male|       006|   Jack|  4000|
    //    | 32|    1|  male|       004|   John|  4000|
    //    | 33|    1|  male|       001|Michael|  3000|
    //    +---+-----+------+----------+-------+------+

    //==========================================
    /*
    增加列
     */

    //select only "name" column
    people.select("age").show()

    //    +---+
    //    |age|
    //    +---+
    //    | 33|
    //    | 30|
    //    | 19|
    //    | 32|
    //    | 20|
    //    | 26|
    //    +---+

    //select everybody,but increment the age by 1
    //选择同时定义
    people.select(people("age"), (people("age")*1.0/100*100)).show()

    //      +---+---------+
    //      |age|(age + 1)|
    //      +---+---------+
    //      | 33|       34|
    //      | 30|       31|
    //      | 19|       20|
    //      | 32|       33|
    //      | 20|       21|
    //      | 26|       27|
    //      +---+---------+






    people.withColumn("level",people("age")/10).show
    //    +---+-----+------+----------+-------+------+-----+
    //    |age|depId|gender|job number|   name|salary|level|
    //    +---+-----+------+----------+-------+------+-----+
    //    | 33|    1|  male|       001|Michael|  3000|  3.3|
    //    | 30|    2|female|       002|   andy|  4000|  3.0|
    //    | 19|    1|  male|       003| Justin|  4000|  1.9|
    //    | 32|    1|  male|       004|   John|  4000|  3.2|
    //    | 20|    2|female|       005|  Herry|  4000|  2.0|
    //    | 26|    3|  male|       006|   Jack|  4000|  2.6|
    //    +---+-----+------+----------+-------+------+-----+


    //==========================================
    /*
    修改列名字
     */
    people.withColumnRenamed("job number","jobId").show()
//    +---+-----+------+-----+-------+------+
//    |age|depId|gender|jobId|   name|salary|
//    +---+-----+------+-----+-------+------+
//    | 33|    1|  male|  001|Michael|  3000|
//    | 30|    2|female|  002|   andy|  4000|
//    | 19|    1|  male|  003| Justin|  4000|
//    | 32|    1|  male|  004|   John|  4000|
//    | 20|    2|female|  005|  Herry|  4000|
//    | 26|    3|  male|  006|   Jack|  4000|
//    +---+-----+------+-----+-------+------+


    //==========================================
    /*
    unionAll操作
     */

    people.unionAll(newpeople).show()
    //
    //    +---+-----+------+----------+-------+------+
    //    |age|depId|gender|job number|   name|salary|
    //      +---+-----+------+----------+-------+------+
    //    | 33|    1|  male|       001|Michael|  3000|
    //    | 30|    2|female|       002|   andy|  4000|
    //    | 19|    1|  male|       003| Justin|  4000|
    //    | 32|    1|  male|       004|   John|  4000|
    //    | 20|    2|female|       005|  Herry|  4000|
    //    | 26|    3|  male|       006|   Jack|  4000|
    //    | 32|    1|  male|       007|   John|  4000|
    //    | 20|    2|female|       008|  Herry|  4000|
    //    | 26|    3|  male|       009|   Jack|  4000|
    //    +---+-----+------+----------+-------+------+


    //==========================================
    /*
    聚合操作groupBy
     */
    people.unionAll(newpeople).groupBy("name").count().show
    //      +-------+-----+
    //      |   name|count|
    //      +-------+-----+
    //      |   Jack|    2|
    //      |   John|    2|
    //      |Michael|    1|
    //      | Justin|    1|
    //      |  Herry|    2|
    //      |   andy|    1|
    //      +-------+-----+

    val depAgg = people.groupBy("depId").agg(
      Map(
        "age"->"max",
        "gender"->"count"
      )
    )
    depAgg.show()
    //+-----+--------+-------------+
    //|depId|max(age)|count(gender)|
    //+-----+--------+-------------+
    //|    1|      33|            3|
    //|    2|      30|            2|
    //|    3|      26|            1|
    //+-----+--------+-------------+
    depAgg.toDF("depId","maxAge","countGender").show()
    //    +-----+------+-----------+
    //    |depId|maxAge|countGender|
    //    +-----+------+-----------+
    //    |    1|    33|          3|
    //    |    2|    30|          2|
    //    |    3|    26|          1|
    //    +-----+------+-----------+


    //==========================================
    /*
    去重操作distinct
     */
    people.unionAll(newpeople).select("name").distinct().show()
    //    +-------+
    //    |   name|
    //    +-------+
    //    |   Jack|
    //    |   John|
    //    |Michael|
    //    | Justin|
    //    |  Herry|
    //    |   andy|
    //    +-------+

    //==========================================
    /*
    交集和异或
     */
    people.select("name").except(newpeople.select("name")).show()
    //    +-------+
    //    |   name|
    //    +-------+
    //    |   andy|
    //    | Justin|
    //    |Michael|
    //    +-------+
    people.select("name").intersect(newpeople.select("name")).show()
    //    +-----+
    //    | name|
    //    +-----+
    //    |Herry|
    //    | John|
    //    | Jack|
    //    +-----+

    //==========================================
    /*
    DataFrame间的join
     */
    people.join(dept,people("depId")===dept("depId"),"inner").show()
    //    +---+-----+------+----------+-------+------+-----+---------------+
    //    |age|depId|gender|job number|   name|salary|depId|           name|
    //      +---+-----+------+----------+-------+------+-----+---------------+
    //    | 33|    1|  male|       001|Michael|  3000|    1|Develoment Dept|
    //    | 19|    1|  male|       003| Justin|  4000|    1|Develoment Dept|
    //    | 32|    1|  male|       004|   John|  4000|    1|Develoment Dept|
    //    | 26|    3|  male|       006|   Jack|  4000|    3|   Testing Dept|
    //    | 30|    2|female|       002|   andy|  4000|    2| Personnel Dept|
    //    | 20|    2|female|       005|  Herry|  4000|    2| Personnel Dept|
    //    +---+-----+------+----------+-------+------+-----+---------------+



    sc.stop()

  }

}
