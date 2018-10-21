package com.ecit.spark_scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Nginx {

  val pattern = "^(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s\\-\\s-\\s(\\[[^\\[\\]]+\\])\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\d{3})\\s(\\d+|-)\\s(\\\"(?:[^\"]|\\\")+|-\\\")\\s(\\\"(?:[^\"]|\\\")+|-\\\")$".r

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("nginx").getOrCreate()
    var accessRdd = spark.read.textFile("hdfs://localhost:9000/data/input/access.log").rdd
    var accesss = accessRdd.filter(line => {
      var logs = parseLog(line)
      if(logs.length <= 0){
        false
      }
	if(null != logs(2)){
		println(logs.mkString(","))
		false
	}
      if(null != logs(2) && logs(2).contains(".html")){
        true
      }
      false
    }).map(line => (line, 1)).reduceByKey(_ + _)
    accesss.saveAsTextFile("hdfs://localhost:9000/data/nginx")
  }

  def onlyStrings(a: String) = {
    var logs = parseLog(a)
    if(logs(2).contains(".html")){
      true
    }
     false;
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
    }

    return array
  }
}
