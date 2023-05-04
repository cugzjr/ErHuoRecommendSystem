package com.xxxx.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jblas.DoubleMatrix
import redis.clients.jedis.Jedis

// 定义一个连接助手对象，建立到redis和mongodb的连接
object ConnHelper extends Serializable{
  // 懒变量定义，使用的时候才初始化
  lazy val jedis = new Jedis("123.249.11.83", 8080)
  jedis.auth("Zjr010205@")
}

case class Product( productId: Int, categories: String )
case class MongoConfig( uri: String, db: String )

// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )

// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )

object ContentRecommender {
  // 定义mongodb中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://root:123456@124.70.143.70:27017/recommender?authSource=admin",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    // 载入数据，做预处理
    val productTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .map(
        x => ( x.productId, x.categories.map(c=> if(c=='|') ' ' else c) )
      )
      .toDF("productId", "categories")
      .cache()

    // TODO: 用TF-IDF提取商品特征向量
    // 1. 实例化一个分词器，用来做分词，默认按照空格分
    val tokenizer = new Tokenizer().setInputCol("categories").setOutputCol("words")
    // 用分词器做转换，得到增加一个新列words的DF
    val wordsDataDF = tokenizer.transform(productTagsDF)

    // 2. 定义一个HashingTF工具，计算频次
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featurizedDataDF = hashingTF.transform(wordsDataDF)

    // 3. 定义一个IDF工具，计算TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练一个idf模型
    val idfModel = idf.fit(featurizedDataDF)
    // 得到增加新列features的DF
    val rescaledDataDF = idfModel.transform(featurizedDataDF)

    // 对数据进行转换，得到RDD形式的features
    val productFeatures = rescaledDataDF.map{
      row => ( row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray )
    }
      .rdd
      .map{
        case (productId, features) => ( productId, new DoubleMatrix(features) )
      }

    // 两两配对商品，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      // 计算余弦相似度
      .map{
      case (a, b) =>
        val simScore = consinSim( a._2, b._2 )
        ( a._1, ( b._1, simScore ) )
    }
      .filter(_._2._2 > 0.3)
      .groupByKey()
      .map{
        case (productId, recs) =>
          ProductRecs( productId, recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    saveDataToRedis(productRecs, ConnHelper.jedis)
    spark.stop()
  }
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double ={
    product1.dot(product2)/ ( product1.norm2() * product2.norm2() )
  }

  def saveDataToRedis(productRecs: DataFrame, jedis: Jedis): Unit = {
    productRecs.collect().foreach { case Row(productId: Int, recs: Seq[Row]) =>
      val redisKey = s"ContentRecommend:$productId"
      if (jedis.exists(redisKey)) {
        jedis.del(redisKey)
      }
      recs.foreach { case Row(productId: Int, score: Double) =>
        jedis.lpush(redisKey, s"$productId")
      }
    }
    jedis.close()
  }
}
