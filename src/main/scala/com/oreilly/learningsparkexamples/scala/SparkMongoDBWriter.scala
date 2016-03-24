//package com.oreilly.learningsparkexamples.scala
//
//
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//
///**
// * Created by user on 2016/2/3.
// */
//object SparkMongoDBWriter {
//  def main(args: Array[String]) {
//    val sparkConf = new SparkConf().setAppName("Spark-MongoDB-Writer")
//    val sc = new SparkContext(sparkConf)
//    //sc.addJar(getClass().getProtectionDomain().getCodeSource().getLocation().getFile() + "../learning-spark-example-1.0-SNAPSHOT.jar")
//    val sqlContext = new HiveContext(sc)
//    val dataFrame: DataFrame = sqlContext.createDataFrame(sc.parallelize(List(Student("Torcuato", 27), Student("Rosalinda", 34))))
//
//    import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
//    import com.stratio.datasource.mongodb.MongodbConfig._
//    import com.stratio.datasource.mongodb._
//    val builder = MongodbConfigBuilder(Map(Host -> List("172.16.91.46:22117"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SplitSize -> 8, SplitKey -> "_id"))
//    val writeConfig = builder.build()
//    dataFrame.saveToMongodb(writeConfig)
//  }
//
//}
//
//case class Student(name: String, age: Int)
//
//
