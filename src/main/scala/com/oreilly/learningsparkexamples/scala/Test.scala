package com.oreilly.learningsparkexamples.scala


import java.util.{Calendar, Date}

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}


/**
 * Created by user on 2016/3/15.
 */
object Test {


  //获取开始结束查询时间
  def getSearchDate: (Date, Date) = {
    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    val endDate = cal.getTime()
    cal.add(Calendar.DAY_OF_MONTH, -1)
    val startDate = cal.getTime()
    (startDate, endDate)
  }


  def main(args: Array[String]) {
    //输入mongdb的数据信息
    val mongodbHostPort = "172.16.91.36:22117"
    val mongodbOutputDatabase = "behavior-analysis-result"
    val mongodbOutputCollection = "behavior-statistics"
    val analysisDate = getSearchDate._1
    val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://" + mongodbHostPort))
    val db = mongoClient(mongodbOutputDatabase)
    val coll = db(mongodbOutputCollection)
    val query = MongoDBObject(("appName", "weixin-consultation"), ("analysisType", "readChannel"), ("analysisDate", analysisDate))

    val docs = coll.find(query)
    if (docs.count > 0) coll.remove(query)
//    println(docs.count)
//
//    for (doc <- docs) println(doc)


    //    val results: Seq[Document] = Await.result(collection.find().limit(10).toFuture(), Duration(10, TimeUnit.SECONDS))
    //    val converter: (Document) => String = (doc) => doc.toJson
    //    results.foreach(res => println(converter(res)))


  }

}
