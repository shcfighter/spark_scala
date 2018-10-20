package com.ecit.spark_scala

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.PrintWriter
import util.control.Breaks._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Connection
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import java.util.Properties
import org.apache.spark.sql.SaveMode

object SimpleDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "test")
    val sqlc = new SQLContext(sc)
    val driverUrl = "jdbc:mysql://localhost:3306/netty_rest?characterEncoding=utf-8"
    val tableName = "user"

    //从MYSQL读取数据
    val jdbcDF = sqlc.read
      .options(Map(
        "driver" -> "com.mysql.cj.jdbc.Driver", 
        "url" -> driverUrl,
        "user" -> "root",
        "password" -> "Hh123!456",
        "dbtable" -> tableName))
      .format("jdbc")
      .load()
    println(jdbcDF.count() + "---------------------------")
    jdbcDF.collect().map { row =>
      {
        println(row.toString())
      }
    }

    println("===============================================================")
    
    val userDF = jdbcDF.where("id = 3")
    userDF.collect().map { row =>
      {
        println(row.toString())
      }
    }
  }

}
