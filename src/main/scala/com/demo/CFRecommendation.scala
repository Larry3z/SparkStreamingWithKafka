package com.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

import scala.collection.Map


object CFRecommendation {

  /**
    * 根据用户Id和model为用户推荐K个物品
    * @param userId
    * @param K
    * @param model
    * @param titles
    */
  def recommedKItemForUser(userId: Int, K: Int, model: MatrixFactorizationModel, titles: Map[Int, String]) = {
    val topKRecs = model.recommendProducts(userId, K)
    println("为用户ID:"+userId+" 推荐"+K+"部电影如下...")
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)
  }

  /**
    * 根据model和itemId查询K个相近的物品
    * @param itemId
    * @param K
    * @param model
    * @param titles
    * @return
    */
  def findSimiliarKItem(itemId:Int,K: Int, model: MatrixFactorizationModel,titles: Map[Int, String]) ={
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    val sims = model.productFeatures.map { case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    val sortedSims = sims.top(K + 1)(
        Ordering.by[(Int, Double), Double] {
          case (id, similarity) => similarity
        }
    )
    println("与物品ID:"+itemId+" 相似的"+K+"物品如下...")
    sortedSims.slice(1, K + 1).map { case (id, sim) => (titles(id), sim) }.foreach(println)
  }

  /**
    * 计算MSE均方差 Mean Squared Error
    * @param ratings
    * @param model
    */
  def caculateMSE(ratings:RDD[Rating], model: MatrixFactorizationModel) ={
    //(user,product) key-value
    val usersProducts = ratings.map { case Rating(user, product, rating) => (user, product) }
    //predict
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rating) => ((user, product), rating)
    }
    //actual rate join predict rate
    //key-value:((user,product),(actual rate, predicted rate))
    val ratingsAndPredictions = ratings.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)
    val MSE = ratingsAndPredictions.map {
      case ((user, product), (actual, predicted)) => math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)
    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)
  }
  /**
    * 计算余弦相似度
    * @param vec1
    * @param vec2
    * @return
    */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("CFRecommendation").setMaster("local")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("file/ml-100k/u.data") //读取原始数据
    //把原始数据转换成Rating
    val ratings = rawData.map(_.split("\t").take(3) match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })
    //用ALS构建推荐模型
    val rank = 100
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //------为用户推荐物品
    val userId = 789
    val movieId = 123
    val predictedRating789 = model.predict(userId, movieId) //预测用户ID为789对电影ID为123的评分
    println("预测用户ID:" + userId + "对电影ID: " + movieId + "的评分 : " + predictedRating789)
    val K = 5
    val movies = sc.textFile("file/ml-100k/u.item") //读取电影的信息
    //ID,name
    val titles = movies.map(_.split("\\|").take(2))
      .map(array => (array(0).toInt, array(1))).collectAsMap()
    println("电影ID为" + movieId + "的电影名称 ：" + titles(movieId))
    val moviesForUser = ratings.keyBy(_.user).lookup(userId)
    println("用户"+userId+"实际看过了多少部电影"+moviesForUser.size)
    println("用户ID:"+userId+" 实际看过的"+K+"部电影和评分如下...")
    //输出用户实际看过的K部电影，按照评分高低排序
    moviesForUser.sortBy(-_.rating).take(K).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    //为用户推荐的K部电影
    recommedKItemForUser(userId,K,model,titles)
    //---end---为用户推荐物品



    //-----找出相似物品
    val itemId = 567
    println("物品ID:567名称是"+titles(itemId))
    findSimiliarKItem(itemId,K,model,titles)
    //---end---找出相似物品


    val actualRating = moviesForUser.take(1)(0)
    val predictedRating = model.predict(789, actualRating.product)
    val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)

    //计算MSE
    caculateMSE(ratings,model)
  }

}
