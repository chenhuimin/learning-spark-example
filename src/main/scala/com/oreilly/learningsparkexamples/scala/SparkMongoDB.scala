package com.oreilly.learningsparkexamples.scala


import java.util.{HashMap => _}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._


/**
 * Created by user on 2016/2/3.
 */
object SparkMongoDB {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark-MongoDB")
    val sc = new SparkContext(sparkConf)
    //sc.addJar(getClass().getProtectionDomain().getCodeSource().getLocation().getFile() + "../learning-spark-example-1.0-SNAPSHOT.jar")
    val hiveContext = new HiveContext(sc)
    val dataFrame: DataFrame = hiveContext.createDataFrame(sc.parallelize(List(Student("Torcuato", 27), Student("Rosalinda", 34))))
    import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
    import com.stratio.datasource.mongodb.MongodbConfig._
    import com.stratio.datasource.mongodb._
     import com.mongodb.{WriteConcern=>_}


    val saveConfig = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
    dataFrame.saveToMongodb(saveConfig.build)
  }

}

case class Student(name: String, age: Int)


