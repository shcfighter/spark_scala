package com.ecit.spark_scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Nginx {

  val pattern = "^(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s\\-\\s-\\s(\\[[^\\[\\]]+\\])\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\d{3})\\s(\\d+|-)\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\\"(?:[^\"]|\\\")+|-\\\")$".r

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("nginx").getOrCreate()
    var accessRdd = spark.read.textFile("hdfs://localhost:9000/data/input/access.log").rdd
    var accesss = accessRdd.map(a => parseLog(a))
      .filter(line => {
      println("==========================================")
      var arrayLog = line.split(",")
      if(arrayLog(2).contains(".html")){
        return true
      }
      return false;
    })
    accesss.saveAsTextFile("hdfs://localhost:9000/data/nginx")
  }

  def parseLog(log: String): Array[String] = {
    var data = pattern.findAllIn(log).matchData
    var array = new Array[String](7);
    data.foreach(m => {
      array(0) = m.group(1)
      array(1) = m.group(2)
      array(2) = m.group(3)
      array(3) = m.group(4)
      array(4) = m.group(5)
      array(5) = m.group(6)
      array(6) = m.group(7)
    })
    println("------------------------")
    return array
  }
}
