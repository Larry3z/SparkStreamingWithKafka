package com.demo

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //关闭jetty日志
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.OFF)
    //kafka配置文件
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
      "group.id" -> "spark-maxwell",
      "enable.auto.commit" -> (true: java.lang.Boolean),
      "auto.commit.interval.ms" -> "1000",
      "session.timeout.ms" -> "30000",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )
    val sparkConf = new SparkConf().setAppName("TestKafka").setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))
    val topics = Array("maxwell")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //({"database":"jeeshop","table":"t_product","pk.id":10326},{"database":"jeeshop","table":"t_product",
    //  "type":"update","ts":1482202509,"xid":19066488,"commit":true,"data":{"id":10326,"name":"Cocktail Classics",
    //  "introduce":"David Biggs,2004","price":24.00,"nowPrice":22.00,"picture":"images.amazon
    // .com/images/P/1845170423.01.LZZZZZZZ.jpg","createtime":"2016-11-03 10:27:34","createAccount":"admin","updateAccount":"admin","updatetime":"2016-11-03 10:27:40","isnew":"y","sale":"n","hit":10187,"status":2,"productHTML":"David Biggs,2004","maxPicture":null,"images":null,"catalogID":"94","sellcount":0,"stock":143,"searchKey":null,"title":null,"description":null,"keywords":null,"activityID":null,"unit":"item","score":0,"isTimePromotion":"n","giftID":null},"old":{"hit":10186}})

    //stream.map(record => (record.key, record.value)).filter(_._1.contains("t_comment")).print()

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
            })
          }
        })
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

