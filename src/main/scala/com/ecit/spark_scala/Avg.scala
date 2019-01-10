package com.ecit.spark_scala

import org.apache.spark.{SparkConf, SparkContext}

object Avg {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf();
    var sc = new SparkContext(conf);
    var rdd = sc.textFile("file:///root/book.txt");
    var rdd2 = rdd.map(line => line.split(","))
    rdd2.foreach(line => println(line))
    println("================================================================================")
    //rdd2.map(line => (line(0), line(1))).mapValues(line => (line, 1)).reduceByKey((x, y) => (x + y) / (x + y)).collect()

    sc.parallelize(List(("a",10),("b",4),("a",10),("b",20))).mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1, x._2, x._1.toDouble / x._2.toDouble)).collect().foreach(line => println(line))
  }
}
