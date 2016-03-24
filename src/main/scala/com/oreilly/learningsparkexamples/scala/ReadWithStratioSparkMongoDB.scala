package com.oreilly.learningsparkexamples.scala

//import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.mongodb.DBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import com.mongodb.casbah.commons.MongoDBObject
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import tachyon.{Constants, TachyonURI}
import tachyon.client.ClientContext
import tachyon.client.file.options.DeleteOptions
import tachyon.client.file.options.DeleteOptions.Builder
import tachyon.client.file.{TachyonFile, TachyonFileSystem}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.util.Config
import org.apache.spark.sql.types._

/**
  * Created by user on 2016/2/3.
  */
object ReadWithStratioSparkMongoDB {
  val userBehaviorSchema = StructType(StructField("appName", StringType, true) :: StructField("eventType", StringType, true) :: StructField("createTime", TimestampType, true) :: StructField("userId", StringType, true) :: StructField("userName", StringType, true) :: StructField("newsId", StringType, true) :: StructField("newsTitle", StringType, true) :: StructField("url", StringType, true) :: StructField("client", StringType, true) :: StructField("version", StringType, true) :: StructField("readFrom", StringType, true) :: Nil)

  def executeSqlAndSave(sqlc: SQLContext, sql: String, outputCollection: MongoCollection, query: DBObject, mongoConfig: Config): Unit = {
    val statisDF = sqlc.sql(sql)
    outputCollection.remove(query)
    statisDF.saveToMongodb(mongoConfig)
  }

  def getMongoCollection(mongodbHostPort: String, mongodbDatabase: String, mongodbCollection: String): MongoCollection = {
    val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://" + mongodbHostPort))
    val db = mongoClient(mongodbDatabase)
    val collection = db(mongodbCollection)
    collection
  }

  //  def getMongoConfig(mongodbHostPort: String, mongodbDatabase: String, mongodbCollection: String): Config = {
  //    val builder = MongodbConfigBuilder(Map(Host -> List(mongodbHostPort), Database -> mongodbDatabase, Collection -> mongodbCollection, SamplingRatio -> 1.0, WriteConcern -> "normal"))
  //    builder.build()
  //  }

  def ifExists(tachyonFileSystem: TachyonFileSystem, file: String): Boolean = {
    val dataFileURI = new TachyonURI(file)
    val dataFile: Option[TachyonFile] = Option(tachyonFileSystem.openIfExists(dataFileURI))
    if (dataFile.isDefined) true else false
  }

  def deleteFile(tachyonFileSystem: TachyonFileSystem, file: String): Unit = {
    val dataFile: Option[TachyonFile] = Option(tachyonFileSystem.openIfExists(new TachyonURI(file)))
    val deleteOptions: DeleteOptions = new Builder(ClientContext.getConf).setRecursive(true).build
    tachyonFileSystem.delete(dataFile.get, deleteOptions)
  }

  //获取结束查询时间
  def getSearchDate(addDays: Int): Date = {
    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.DAY_OF_MONTH, addDays)
    val endDate = cal.getTime()
    endDate
  }

  def formatDate(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(date)
  }

  def getTFSystem(tachyonMaster: String): TachyonFileSystem = {
    val mMasterLocation = new TachyonURI(tachyonMaster)
    val mTachyonConf = ClientContext.getConf()
    mTachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost)
    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort))
    mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, "false")
    ClientContext.reset(mTachyonConf)
    val tachyonFileSystem: TachyonFileSystem = TachyonFileSystem.TachyonFileSystemFactory.get()
    tachyonFileSystem
  }


  def main(args: Array[String]) {

    val master = "spark://node2.laanto.com:7077"
    val conf = new SparkConf().setMaster(master).setAppName("spark-mongodb").set("spark.executor.memory", "1024M").set("spark.executor.cores", "3").set("spark.cores.max", "9")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    import sqlc.implicits._

    //输入mongdb的数据信息
    val mongodbHostPortPro = "172.16.91.36:22117"
    val mongodbHostPortPrePro = "172.16.91.46:22117"
    val mongodbInputDatabase = "user-behavior"
    val mongodbInputCollection = "weixin-consultation"
    val mongodbOutputDatabase = "behavior-analysis-result"
    val mongodbOutputCollection = "behavior-statistics"
    //    val tachyonHost = "tachyon://node3.laanto.com:19998"
    //    val tachyonFileBasePath = "/data/raw/userBehaviors/full/"
    //    val clearCache = java.lang.Boolean.valueOf("false")
    //    val currentDate = getSearchDate(0)
    //    val tachyonFile = tachyonFileBasePath + s"${formatDate(currentDate)}" + ".parquet"
    //    val tfSystem = getTFSystem(tachyonHost)
    //    if (clearCache) {
    //      if (ifExists(tfSystem, tachyonFile)) {
    //        deleteFile(tfSystem, tachyonFile)
    //      }
    //    }
    //    var userBehaviorDF: DataFrame = null
    //    //判断文件是否存在
    //    if (!ifExists(tfSystem, tachyonFile)) {
    //      //tachyon中不存在，则重新读取mongodb，生成tachyon文件并保存
    //      val readConfig = getMongoConfig(mongodbHostPortPro, mongodbInputDatabase, mongodbInputCollection)
    //      userBehaviorDF = sqlc.fromMongoDB(readConfig, Some(userBehaviorSchema))
    //      userBehaviorDF.write.parquet(tachyonHost + tachyonFile)
    //    } else {
    //      //tachyon存在，则直接从tachyon中读取
    //      userBehaviorDF = sqlc.read.parquet(tachyonHost + tachyonFile)
    //    }
    val proReadConfig = MongodbConfigBuilder(Map(Host -> List(mongodbHostPortPro), Database -> mongodbInputDatabase, Collection -> mongodbInputCollection, SamplingRatio -> 0.1, CursorBatchSize -> 1000)).build()
    val userBehaviorsDF = sqlc.fromMongoDB(proReadConfig, Some(userBehaviorSchema))
    // userBehaviorsDF.where(userBehaviorsDF("createTime") > new java.sql.Timestamp(getSearchDate(0).getTime)).registerTempTable("userBehaviors")
    userBehaviorsDF.registerTempTable("userBehaviors")
    sqlc.cacheTable("userBehaviors")

    //设置shuffle数
    sqlc.sql("SET spark.sql.shuffle.partitions=20")

    //sql1：统计微店热度
    val statisShopSql =
      """select appName,
        |to_date(createTime) statisDate,
        |count(distinct userId) uv,
        |count(*) pv,
        |'1001' shopId,
        |'statisShop' statisType
        |from userBehaviors
        |where eventType='1'
        |and newsId is not null
        |and userId is not null and userId <> '-1'
        |and createTime >= current_date()
        |and createTime <= current_timestamp
        |group by appName, to_date(createTime)""".stripMargin.replaceAll("\n", " ")

    val outputCollection = getMongoCollection(mongodbHostPortPro, mongodbOutputDatabase, mongodbOutputCollection)
    val statisShopQuery = MongoDBObject(("appName", "weixin-consultation"), ("shopId", "1001"), ("statisType", "statisShop"), ("statisDate", getSearchDate(0)))
    val proSaveConfig = MongodbConfigBuilder(Map(Host -> List(mongodbHostPortPro), Database -> mongodbOutputDatabase, Collection -> mongodbOutputCollection, SamplingRatio -> 0.1, WriteConcern -> "normal", CursorBatchSize -> 1000)).build()
    executeSqlAndSave(sqlc, statisShopSql, outputCollection, statisShopQuery, proSaveConfig)

    //sql2：统计微店产品热度
    val statisShopProductSql =
      """select appName,
        |newsId,
        |count(distinct userId) uv,
        |count(*) pv,
        |'1001' shopId,
        |'statisShopProduct' statisType
        |from userBehaviors
        |where eventType='1'
        |and newsId is not null
        |and userId is not null and userId <> '-1'
        |group by appName, newsId
        |order by uv desc, pv desc""".stripMargin.replaceAll("\n", " ")

    val statisShopProductQuery = MongoDBObject(("appName", "weixin-consultation"), ("shopId", "1001"), ("statisType", "statisShopProduct"))
    executeSqlAndSave(sqlc, statisShopProductSql, outputCollection, statisShopProductQuery, proSaveConfig)

    //sql3：统计微店最近访客
    val statisShopVisitorSql =
      """select
        |appName,
        |userId,
        |max(createTime) visitTime,
        |'1001' shopId,
        |'statisShopVisitor' statisType
        |from userBehaviors
        |where eventType='1'
        |and newsId is not null
        |and userId is not null and userId <> '-1'
        |group by appName, userId
        |order by max(createTime) desc
        |limit 100""".stripMargin.replaceAll("\n", " ")

    val statisShopVisitorQuery = MongoDBObject(("appName", "weixin-consultation"), ("shopId", "1001"), ("statisType", "statisShopVisitor"))
    executeSqlAndSave(sqlc, statisShopVisitorSql, outputCollection, statisShopVisitorQuery, proSaveConfig)
    sqlc.uncacheTable("userBehaviors")

  }
}




