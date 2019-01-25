package com.ecit.spark_steaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object WindowBasedTopWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowBasedTopWord").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5)) //这里的5秒是指切分RDD的间隔
    ssc.checkpoint("hdfs://localhost:9000/wordcount_checkpoint") //设置docheckpoint目录,没有会自动创建

    val words = ssc.socketTextStream("localhost",8888) //可以从kafka集群中获取信息
    val pairs = words.flatMap(_.split(" ")).map(x => (x,1))

    pairs.foreachRDD(rdd => {
      println("--------------split RDD begin--------------")
      rdd.foreach(println)
      println("--------------split RDD end--------------")
    })
    /*
    reduceByKeyAndWindow(reduceFunc,invReduceFunc,windowDuration,slideDuration)
    reduceFunc：用于计算window框住的RDDS
    invReduceFunc：用于优化的函数，减少window滑动中去计算重复的数据，通过“_-_”即可优化
    windowDuration：表示window框住的时间长度，如本例5秒切分一次RDD，框20秒，就会保留最近4次切分的RDD
    slideDuration：表示window滑动的时间长度，即每隔多久执行本计算
    本例5秒切分一次RDD，每次滑动10秒，window框住20秒的RDDS，即：每10秒计算最近20秒切分的RDDS，中间有10秒重复，
    通过invReduceFunc参数进行去重优化
     */
    val pairsWindow = pairs.reduceByKeyAndWindow(_+_,_-_,Durations.seconds(20),Durations.seconds(10))
    val sortDstream = pairsWindow.transform(rdd => {
      val sortRdd = rdd.map(t => (t._2,t._1)).sortByKey(false).map(t => (t._2,t._1))//降序排序
      val more = sortRdd.take(3)//取前3个输出
      println("--------------print top 3 begin--------------")
      more.foreach(println)
      println("--------------print top 3 end--------------")
      sortRdd
    })
    sortDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
