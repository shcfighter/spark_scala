package com.ecit.spark_scala

import org.apache.spark.{SparkConf, SparkContext}

object Avg {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf();
    var sc = new SparkContext(conf);
    var rdd = sc.textFile("file:///root/book.txt");
    var rdd2 = rdd.map(line => line.split(","))
    rdd2.foreach(line => println(line))
  }
}
