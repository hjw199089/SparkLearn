package com.dt.spark.main.RDDLearn.RDDSortAPI

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by  on 16/7/17.
  */

//==========================================
/*
http://blog.csdn.net/jiangpeng59/article/details/52938465

 */

object RDDSortAPI {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    conf.setAppName("test")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //==========================================
    /*
    1.sortByKey
    无可非议sortByKey是Spark的最常用的排序，简单的案例暂且跳过，下面给一个非简单的案例，进入排序之旅
    对下面简单元祖，要求先按元素1升序，若元素1相同，则再按元素3升序
     (1, 6, 3), (2, 3, 3), (1, 1, 2), (1, 3, 5), (2, 1, 2)
    提示：sortByKey对于key是单个元素排序很简单，如果key是元组如(X1，X2，X3.....)，它会先按照X1排序，若X1相同，则在根据X2排序，依次类推...
     */
    val array = Array((1, 6, 3), (2, 3, 3), (1, 1, 2), (1, 3, 5), (2, 1, 2))
    val rdd1 = sc.parallelize(array)
//    //设置元素(e1,e3)为key,value为原来的整体
//    val rdd2 = rdd1.map(f => ((f._1, f._3), f))
//    //利用sortByKey排序的对key的特性
//    val rdd3 = rdd2.sortByKey()
//    rdd3.foreach(println(_))
//    //    ((1,2),(1,1,2))
//    //    ((1,3),(1,6,3))
//    //    ((1,5),(1,3,5))
//    //    ((2,2),(2,1,2))
//    //    ((2,3),(2,3,3))
//    val rdd4 = rdd3.values.collect
//
//    //rdd4: Array[(Int, Int, Int)] = Array((1,1,2), (1,6,3), (1,3,5), (2,1,2), (2,3,3))

    //=====
    /*
        2.sortBy
    SortBy其实是SortBykey的加强版，比如上面的功能可以使用这个函数实现
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    val rdd2=rdd1.sortBy(f=>(f._1,f._3)).collect
    看上去是不是很神奇，其实sortBy内部帮我们做的，就是我上面写的代码。下面看下SortBy的源码：
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    def sortBy[K](
         f: (T) => K,
         ascending: Boolean = true,
         numPartitions: Int = this.partitions.length)
         (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
       this.keyBy[K](f)
           .sortByKey(ascending, numPartitions)
           .values
     }

    sortBy先调用keyBy函数，而keyBy的功能很简单，key为用户制定，比如上面的f => ((f._1, f._3)，value为原始值：
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    def keyBy[K](f: T => K): RDD[(K, T)] = withScope {
       val cleanedF = sc.clean(f)
       map(x => (cleanedF(x), x))
     }
    最后在调用sortByKey函数，和上面的神似有木有..
     */
    val rdd2=rdd1.sortBy(f=>(f._1,f._3)).collect
    rdd2.foreach(println _)
    //    (1,1,2)
    //    (1,6,3)
    //    (1,3,5)
    //    (2,1,2)
    //    (2,3,3)
    val rdd3=rdd1.sortBy(f=>(f._1),false).collect
    rdd3.foreach(println _)
    //    (2,3,3)
    //    (2,1,2)
    //    (1,6,3)
    //    (1,1,2)
    //    (1,3,5)
    //=====
    /*
    3.Ordering
    Ordering在Spark的排序应用中随处可见，比如上面的SortBy它就有一个隐式参数implicit ord: Ordering[K]，如下摘抄自Scala提供AIP
    对简单的类型排序, quickSort 这里使用了高阶函数的柯里化，  为第二个括号为Ordering类型的隐式参数
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    import scala.util.Sorting
    val pairs = Array(("a", 5, 2), ("c", 3, 1), ("b", 1, 3))
    // sort by 2nd element
    Sorting.quickSort(pairs)(Ordering.by[(String, Int, Int), Int](_._2))
    // sort by the 3rd element, then 1st
    Sorting.quickSort(pairs)(Ordering[(Int, String)].on(x => (x._3, x._1)))

    对复杂类型的排序
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    import scala.util.Sorting
    case class Person(name:String, age:Int)
    val people = Array(Person("bob", 30), Person("ann", 32), Person("carl", 19))
    // sort by age
    object AgeOrdering extends Ordering[Person] {
      def compare(a:Person, b:Person) = a.age compare b.age
    }
    Sorting.quickSort(people)(AgeOrdering)

    因为sortByKey实现了Ordering的很多功能，下面以Spark中的top函数为例
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    def top(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
        takeOrdered(num)(ord.reverse)
      }
    在下面的元组中，以第2个元素基，取出前3大的元组
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    val array = Array((1, 3, 3), (2, 6, 3), (1, 1, 2), (1, 5, 4), (2, 1, 2))
    val rdd1 = sc.parallelize(array)
    val result=rdd1.top(3)(Ordering.by[(Int, Int, Int), Int](_._2))
    结果：
    [java] view plain copy 在CODE上查看代码片派生到我的代码片
    result: Array[(Int, Int, Int)] = Array((2,6,3), (1,5,4), (1,3,3))
     */

    sc.stop()

  }

}
