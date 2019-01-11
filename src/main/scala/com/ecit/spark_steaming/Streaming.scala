package com.ecit.spark_steaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Streaming {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SPARK_STREAM")
    var sc = new SparkContext(conf)
    var streamingContext = new StreamingContext(sc, Seconds(30))
    var ds = streamingContext.textFileStream("file:///root/logfile/")
    var ds2 = ds.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    ds2.foreachRDD(_.foreach(println(_)))
  }
}
