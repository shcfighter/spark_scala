package com.ecit.spark_sql

import org.apache.spark.sql.SparkSession

object TestSql {

  def main(args: Array[String]): Unit = {
    var sparkSession = SparkSession.builder().appName("app_spark_sql").getOrCreate();
    var df = sparkSession.read.json("file:///root/people.json");
    df.createTempView("people");
    df.select("name", "age").show();
    println("================================================================");
    sparkSession.sql("select * from people").show();
  }

}
