package com.oreilly.learningsparkexamples.scala

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by user on 2016/2/3.
 */
object SparkMongoDBReader {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark-MongoDB-Reader")
    val sc = new SparkContext(sparkConf)
    //sc.addJar(getClass().getProtectionDomain().getCodeSource().getLocation().getFile() + "../learning-spark-example-1.0-SNAPSHOT.jar")
    val sqlContext = new HiveContext(sc)
    import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
    import com.stratio.datasource.mongodb.MongodbConfig._
    import com.stratio.datasource.mongodb._


    val builder = MongodbConfigBuilder(Map(Host -> List("172.16.91.46:22117"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal))
    val readConfig = builder.build()

    val mongoRDD = sqlContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("students")
    sqlContext.sql("SELECT name, age FROM students")


  }

}




