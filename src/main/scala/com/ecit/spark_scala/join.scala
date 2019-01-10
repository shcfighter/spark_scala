package com.ecit.spark_scala

import org.apache.spark.{SparkConf, SparkContext}

object Join {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf();
    var sc = new SparkContext(conf);
    var rdd1 = sc.textFile("file:///root/people.txt")
    var rdd2 = sc.textFile("file:///root/p.txt")
    var rdd22 = rdd2.map(line => (line, "fast"))
    rdd1.map(line => (line, 1)).join(rdd22).foreach(result => println())
  }
}

