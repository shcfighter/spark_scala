

package com.ecit.spark_scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile("hdfs://localhost:9000/data/input/shop-user.log")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://localhost:9000/data/user")
  }
}