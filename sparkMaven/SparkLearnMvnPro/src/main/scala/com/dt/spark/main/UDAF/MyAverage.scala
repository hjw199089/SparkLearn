//package com.dt.spark.main.UDAF
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types._
//import org.apache.spark.{SparkConf, SparkContext}
//
//object MyAverage extends UserDefinedAggregateFunction {
//  // Data types of input arguments of this aggregate function
//  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
//  // Data types of values in the aggregation buffer
//  def bufferSchema: StructType = {
//    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
//  }
//  // The data type of the returned value
//  def dataType: DataType = DoubleType
//  // Whether this function always returns the same output on the identical input
//  def deterministic: Boolean = true
//  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
//  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
//  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
//  // immutable.
//  def initialize(buffer: MutableAggregationBuffer): Unit = {
//    buffer(0) = 0L
//    buffer(1) = 0L
//  }
//  // Updates the given aggregation buffer `buffer` with new input data from `input`
//  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    if (!input.isNullAt(0)) {
//      buffer(0) = buffer.getLong(0) + input.getLong(0)
//      buffer(1) = buffer.getLong(1) + 1
//    }
//  }
//  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
//  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
//    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
//  }
//  // Calculates the final result
//  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
//
//  def main(args: Array[String]) {
//    val conf = new SparkConf()
//    conf.setAppName("test")
//    conf.setMaster("local")
//
//    val sc = new SparkContext(conf)
//
//    //设置日志级别
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
//
//    val sqlContext = new HiveContext(sc)
//
//    // Register the function to access it
//    sqlContext.udf.register("myAverage", MyAverage)
//
//    val df = sqlContext.read.json("./src/main/java/com/dt/spark/main/UDAF/employee.json")
//    df.registerTempTable("employees")
//    df.show()
//    // +-------+------+
//    // |   name|salary|
//    // +-------+------+
//    // |Michael|  3000|
//    // |   Andy|  4500|
//    // | Justin|  3500|
//    // |  Berta|  4000|
//    // +-------+------+
//
//    val result = sqlContext.sql("SELECT myAverage(salary) as average_salary FROM employees")
//    result.show()
//    // +--------------+
//    // |average_salary|
//    // +--------------+
//    // |        3750.0|
//    // +--------------+
//
//  }
//}
//
//
//
//
