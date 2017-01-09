package com.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}


object SparkMysql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.OFF)
    //关闭jetty日志
    val sparkConf = new SparkConf().setAppName("SparkMysql").setMaster("local[2]")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/jeeshop?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", "t_comment")
      .option("user", "root")
      .option("password", "XXXXXX")
      .load().cache()
    //jdbcDF.rdd.foreach(print)
    val ratings = jdbcDF.rdd.map(row => Rating(row.getString(2).toInt, row.getString(1).toInt, row.getInt(8)))
    //用ALS构建推荐模型
    val rank = 100
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    model.userFeatures.foreach(println)
  }
}
