package com.ecit.spark_scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Nginx {

  val pattern = "^(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s\\-\\s-\\s(\\[[^\\[\\]]+\\])\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\d{3})\\s(\\d+|-)\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\\"(?:[^\"]|\\\")+|-\\\")$".r

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("nginx").getOrCreate()
    var accessRdd = spark.read.textFile("hdfs://localhost:9000/data/input/access.log").rdd
    var accesss = accessRdd.map(a => parseLog(a)).filter(line => {
      var arrayLog = line.split(",")
      if(arrayLog(2).contains(".html")){
        return true
      }
      return false;
    })
    accesss.saveAsTextFile("hdfs://localhost:9000/data/nginx")
  }

  def parseLog(log: String): String = {
    var data = pattern.findAllIn(log).matchData
    var array = new Array[String](7);
    data.foreach(m => {
      array += m.group(1)
      array += m.group(2)
      array += m.group(3)
      array += m.group(4)
      array += m.group(5)
      array += m.group(6)
      array += m.group(7)
    })
    return array.mkString(",")
  }
}
