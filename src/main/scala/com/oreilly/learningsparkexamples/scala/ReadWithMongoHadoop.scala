package com.oreilly.learningsparkexamples.scala


import java.lang.Long
import java.sql
import java.sql.Timestamp
import java.util.{Calendar, Date}

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}
import tachyon.client.ClientContext
import tachyon.client.file.options.DeleteOptions
import tachyon.client.file.options.DeleteOptions.Builder
import tachyon.client.file.{TachyonFile, TachyonFileSystem}
import tachyon.{Constants, TachyonURI}


/**
  * Created by user on 2016/2/3.
  */

object ReadWithMongoHadoop {

  case class UserBehavior(appName: String, eventType: String, createTime: Timestamp, userId: String, userName: String, newsId: String, newsTitle: String, url: String, client: String, version: String, readFrom: String)

  //把查询结果存回数据库
  def writeBackToMongoDB(readChannelPairRDD: RDD[(Null, BasicBSONObject)], mongoOutputUriConfig: Configuration): Unit = {
    readChannelPairRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Object], classOf[BSONObject], classOf[MongoOutputFormat[Object, BSONObject]], mongoOutputUriConfig)
  }

  //读取mongo的数据
  def readFromMongoDB(sc: SparkContext, mongoConfig: Configuration): RDD[UserBehavior] = {
    val mongoPairRDD = sc.newAPIHadoopRDD(mongoConfig, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    //把读取的原始Mongo BSONObject数据转换 case class object 对象
    mongoPairRDD.values.map(obj => {
      val appName: String = if (obj.get("appName") != null) obj.get("appName").toString else null
      val eventType: String = if (obj.get("eventType") != null) obj.get("eventType").toString else null
      val createTime: Timestamp = if (obj.get("createTime") != null) new Timestamp(obj.get("createTime").asInstanceOf[Date].getTime()); else null
      val userId: String = if (obj.get("userId") != null) obj.get("userId").toString else null
      val userName: String = if (obj.get("userName") != null) obj.get("userName").toString else null
      val newsId: String = if (obj.get("newsId") != null) obj.get("newsId").toString else null
      val newsTitle: String = if (obj.get("newsTitle") != null) obj.get("newsTitle").toString else null
      val url: String = if (obj.get("url") != null) obj.get("url").toString else null
      val client: String = if (obj.get("client") != null) obj.get("client").toString else null
      val version: String = if (obj.get("version") != null) obj.get("version").toString else null
      val readFrom: String = if (obj.get("readFrom") != null) obj.get("readFrom").toString else null
      UserBehavior(appName, eventType, createTime, userId, userName, newsId, newsTitle, url, client, version, readFrom)
    })
  }

  //设置mongo的查询条件
  def setMongoInputQuery(mongoConfig: Configuration, searchKey: String, searchDate: (Date, Date)): Configuration = {
    val mongoInputQuery: String = """{"""" + searchKey + """":{"$gte":{"$date":""" + s"${searchDate._1.getTime}" + """},"$lt":{"$date":""" + s"${searchDate._2.getTime}" + "}}}"
    mongoConfig.set("mongo.input.query", mongoInputQuery)
    mongoConfig
  }

  //设置mongo数据连接地址
  def setMongoUri(key: String, mongodbHostPort: String, mongodbDatabase: String, mongodbCollection: String): Configuration = {
    val mongoConfig = new Configuration()
    mongoConfig.set(key, "mongodb://" + mongodbHostPort + "/" + mongodbDatabase + "." + mongodbCollection)
    mongoConfig
  }

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

  //获取开始结束查询时间
  def getSearchDate(days: Int): (Date, Date) = {
    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    val endDate = cal.getTime()
    cal.add(Calendar.DAY_OF_MONTH, -days)
    val startDate = cal.getTime()
    (startDate, endDate)
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

  def getMongoCollection(mongodbHostPort: String, mongodbOutputDatabase: String, mongodbOutputCollection: String): MongoCollection = {
    val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://" + mongodbHostPort))
    val db = mongoClient(mongodbOutputDatabase)
    val collection = db(mongodbOutputCollection)
    collection
  }

  def main(args: Array[String]) {
    val master = "spark://node2.laanto.com:7077"
    val conf = new SparkConf().setMaster(master).setAppName("DeepMongodbTest").set("spark.executor.memory", "1024M").set("spark.executor.cores", "3").set("spark.cores.max", "9")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    import sqlc.implicits._

    //输入mongdb的数据信息
    val mongodbHostPort = "172.16.91.36:22117"
    val mongodbInputDatabase = "user-behavior"
    val mongodbInputCollection = "weixin-consultation"
    val mongodbOutputDatabase = "behavior-analysis-result"
    val mongodbOutputCollection = "behavior-statistics"
    val tachyonHost = "tachyon://node3.laanto.com:19998"
    val tachyonFileBasePath = "/data/raw/userBehaviors/"
    val clearCache = java.lang.Boolean.valueOf("false")
    val searchDate = getSearchDate(1)
    val tachyonFile = tachyonFileBasePath + s"${searchDate._1.getTime}" + ".parquet"
    val tfSystem = getTFSystem(tachyonHost)
    if (clearCache) {
      if (ifExists(tfSystem, tachyonFile)) {
        deleteFile(tfSystem, tachyonFile)
      }
    }
    var userBehaviorDF: DataFrame = null
    //判断文件是否存在
    if (!ifExists(tfSystem, tachyonFile)) {
      //tachyon中不存在，则重新读取mongodb，生成tachyon文件并保存
      val mongoInputUriConfig = setMongoUri("mongo.input.uri", mongodbHostPort, mongodbInputDatabase, mongodbInputCollection)
      val mongoInputConfig = setMongoInputQuery(mongoInputUriConfig, "createTime", searchDate)
      val userBehaviorRDD = readFromMongoDB(sc, mongoInputConfig)
      userBehaviorDF = userBehaviorRDD.toDF
      userBehaviorDF.write.parquet(tachyonHost + tachyonFile)
    } else {
      //tachyon存在，则直接从tachyon中读取
      userBehaviorDF = sqlc.read.parquet(tachyonHost + tachyonFile)
    }

    userBehaviorDF.registerTempTable("userBehaviors")

    //设置shuffle数
    sqlc.sql("SET spark.sql.shuffle.partitions=20")
    //sql语句:浏览资讯的浏览渠道分布(0：微信公众号，1：好友对话分享，2：群组内分享，3：朋友圈分享,4: 安卓app，5: iso app)
    val readChannelSql =
      """SELECT to_date(createTime) date, readFrom channel, count(*) readCount
        |FROM userBehaviors
        |WHERE appName='weixin-consultation'
        |AND eventType='1'
        |AND newsId is not null
        |AND readFrom is not null
        |AND readFrom IN ('0','1','2','3','4','5')
        |GROUP BY to_date(createTime), readFrom
        |ORDER BY channel""".stripMargin.replaceAll("\n", " ")
    // 把查询的结果转换成mongo的bsonObject
    var analysisType = "readChannel"
    var appName = "weixin-consultation"
    val readChannelRDD = sqlc.sql(readChannelSql).rdd.map {
      case Row(analysisDate: sql.Date, channel: String, readCount: Long) => new BasicBSONObject().append("analysisDate", analysisDate).append("channel", channel).append("readCount", readCount).append("analysisType", analysisType).append("appName", appName)
    }
    val readChannelPairRDD = readChannelRDD.map(bson => (null, bson))

    //先执行下mongodb的删除操作，以确保数据库中不存在针对该天的该种分析结果
    val collection = getMongoCollection(mongodbHostPort, mongodbOutputDatabase, mongodbOutputCollection)
    val query = MongoDBObject(("appName", appName), ("analysisType", analysisType), ("analysisDate", searchDate._1))
    collection.remove(query)
    //把查询结果存回数据库
    val mongoOutputUriConfig = setMongoUri("mongo.output.uri", mongodbHostPort, mongodbOutputDatabase, mongodbOutputCollection)
    writeBackToMongoDB(readChannelPairRDD, mongoOutputUriConfig)


  }


}




