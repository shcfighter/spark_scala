package com.ecit.spark_sql

import org.apache.spark.sql.SparkSession

object TestSql {

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("app_spark_sql").getOrCreate()

    var df = spark.read.json("file:///root/people.json")
    df.createTempView("people")
    df.select("name", "age").show()
    println("================================================================")
    spark.sql("select * from people").show()

    println("==================================text==============================")
    text(spark)

    //println("==================================text==============================")



  }

  def text(spark: SparkSession): Unit ={
    case class Person(name: String, age: Long)
    import spark.implicits._
    var df = spark.sparkContext
      .textFile("file:///root/people.txt")
      .map(_.split(","))
      .map(arr => Person(arr(1), arr(2).toLong)).toDF()

    df.createTempView("people2")
    spark.sql("select * from people2 where age > 0").show()
  }

}