package com.ecit.spark_scala

import org.apache.spark.sql.SparkSession

object Nginx {

  val pattern = "^(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s\\-\\s-\\s(\\[[^\\[\\]]+\\])\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\d{3})\\s(\\d+|-)\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\\"(?:[^\"]|\\\")+|-\\\")$".r
  val pattern2 = "^(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s(\\[[^\\[\\]]+\\])\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\d{3})\\s(\\d+|-)\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\\"-\\\")$".r

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("nginx").getOrCreate()
    var accessRdd = spark.read.textFile("file:////usr/local/nginx/nginx-1.14.0/webserver/logs/access.log").rdd
    var accesss = accessRdd.map(line => parseLog(line)).filter(arr => {
      if(arr.length <= 0){
        false
      }
      if(null != arr(2)){
        //println(logs.mkString(","))
        false
      }
      /*if(null != logs(2) && logs(2).contains(".html")){
        true
      }*/
      true
    }).map(arr => arr.mkString("\t"))
    accesss.saveAsTextFile("hdfs://localhost:9000/data/nginx")
  }

  def parseLog(log: String): Array[String] = {
    var array = new Array[String](7);
    if(pattern.pattern.matcher(log).matches()){
      var data = pattern.findAllIn(log).matchData
      data.foreach(m => {
        array(0) = m.group(1)
        array(1) = m.group(2)
        array(2) = m.group(3)
        array(3) = m.group(4)
        array(4) = m.group(5)
        array(5) = m.group(6)
        array(6) = m.group(7)
      })
    } else if(pattern2.pattern.matcher(log).matches()){
      var data = pattern2.findAllIn(log).matchData
      data.foreach(m => {
        array(0) = m.group(1)
        array(1) = m.group(2)
        array(2) = m.group(3)
        array(3) = m.group(4)
        array(4) = m.group(5)
        array(5) = m.group(6)
        array(6) = m.group(7)
      })
    }

    return array
  }
}
