package com.demo

import java.sql.{DriverManager, ResultSet}

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


object SparkStreamingKafkaRecommend {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //关闭jetty日志
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("SparkStreamingKafkaRecommend").setMaster("local[2]")
    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/jeeshop?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", "t_comment")
      .option("user", "root")
      .option("password", "XXXXXX")
      .load()
    val ratings = jdbcDF.rdd.map(row => Rating(row.getString(2).toInt, row.getString(2).toInt, row.getInt(8))).cache()
    //用ALS构建推荐模型
    val rank = 100
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //kafka配置文件
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
      "group.id" -> "test-consumer-group",
      "enable.auto.commit" -> (true: java.lang.Boolean),
      "auto.commit.interval.ms" -> "1000",
      "session.timeout.ms" -> "30000",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(2))
    val topics = Array("maxwell")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //只取t_comment表的insert数据
    stream.map(record => (record.key, record.value)).filter(_._1.contains("t_comment")).filter(_._2.contains
    ("insert")).foreachRDD((rdd: RDD[(String, String)], time: Time) => {
      //空rdd不处理
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(par => {
          if (!par.isEmpty) {
            par.foreach(row =>{
              val jsn = JSON.parseObject(row._2)
              val data = jsn.get("data")
              val accountID = JSON.parseObject(data.toString).get("accountID")
              //model.recommendProducts(classOf[String].cast(accountID).toInt,5).foreach(println)
              //org.apache.spark.SparkException: This RDD lacks a SparkContext. It could happen in the following cases:
              //(1) RDD transformations and actions are NOT invoked by the driver, but inside of other
              //  transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because the values
              //  transformation and count action cannot be performed inside of the rdd1.map transformation. For more
              //  information, see SPARK-5063.
              //2) When a Spark Streaming job recovers from checkpoint, this exception will be hit if a reference to
              //an RDD not defined by the streaming job is used in DStream operations. For more information, See
              //SPARK-13758.
              println(model.recommendProducts(classOf[String].cast(accountID).toInt,5))
              model.recommendProducts(classOf[String].cast(accountID).toInt,5).toIterator.foreach(println)
            })
          }
        })
      }
    })
    //streamingContext.checkpoint(checkpointDirectory) // set checkpoint directory
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

