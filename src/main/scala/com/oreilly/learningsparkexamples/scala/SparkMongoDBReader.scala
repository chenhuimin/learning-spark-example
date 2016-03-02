package com.oreilly.learningsparkexamples.scala

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
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

    //sqlContext.setConf("spark.externalBlockStore.url", "tachyon://chenhuimin03.localdomain:19998")

    // 1. how to use the fromMongoDB() function to read from MongoDB and transform it to a DataFrame.

    import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
    import com.stratio.datasource.mongodb.MongodbConfig._
    import com.stratio.datasource.mongodb._
    import org.apache.spark.sql.types._
    import org.apache.spark.storage.StorageLevel

    val builder = MongodbConfigBuilder(Map(Host -> List("172.16.91.36:22117"), Database -> "user-behavior", Collection -> "behavior", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal))
    val readConfig = builder.build()
    val schema = StructType(StructField("appName", StringType, true) :: StructField("createTime", org.apache.spark.sql.types.TimestampType, true) :: StructField("events", MapType(StringType, StringType), true) :: Nil)
    val mongoDF = sqlContext.fromMongoDB(readConfig, Some(schema))
    mongoDF.write.parquet("tachyon://chenhuimin03.localdomain:19998/data/userbehavior.parquet")
    sqlContext.read.parquet("tachyon://chenhuimin03.localdomain:19998/data/userbehavior.parquet")


    mongoDF.persist(StorageLevel.OFF_HEAP)
    mongoDF.registerTempTable("behaviors")
    //sqlContext.cacheTable("behaviors")

    sqlContext.sql("SET spark.sql.shuffle.partitions=20")
    sqlContext.sql("SELECT count(*) FROM behaviors").show()
    val top10 = sqlContext.sql("select events.url,count(1) value from behaviors where appName='weixin-consultation' group by events.url sort by value desc limit 10")
    top10.collect.foreach(println)
    // sqlContext.sql("SELECT appName,createTime,events.uerName,events.userId,events.url,events.eventMsg FROM behaviors WHERE createTime >= CAST('2016-02-14 00:00:00' AS TIMESTAMP) AND createTime < CAST('2016-02-15 00:00:00' AS TIMESTAMP)").show()

    //    val students = sqlContext.sql("SELECT name, age FROM students")
    //    students.collect.foreach(println)

    //  2.  Using StructType:
    //        import org.apache.spark.sql.types._
    //        val schemaMongo = StructType(StructField("name", StringType, true) :: StructField("age", IntegerType, true) :: Nil)
    //        sqlContext.createExternalTable("students", "com.stratio.datasource.mongodb", schemaMongo, Map("host" -> "chenhuimin:27017", "database" -> "highschool", "collection" -> "students"))
    //        sqlContext.sql("SELECT * FROM students WHERE name = 'Torcuato'").show()
    //        sqlContext.sql("DROP TABLE students")

    //3. Using DataFrameWriter:

    //    import org.apache.spark.sql._
    //    val options = Map("host" -> "chenhuimin:27017", "database" -> "highschool", "collection" -> "students")
    //    case class Student(name: String, age: Int)
    //    val dfw: DataFrame = sqlContext.createDataFrame(sc.parallelize(List(Student("Michael", 46))))
    //    dfw.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(options).save()
    //    val df = sqlContext.read.format("com.stratio.datasource.mongodb").options(options).load
    //    df.show

    //4. Using HiveContext (sqlContext in spark-shell provide Hive support):
    //    sqlContext.sql("CREATE TABLE IF NOT EXISTS students(name STRING, age INTEGER) USING com.stratio.datasource.mongodb OPTIONS (host 'chenhuimin:27017', database 'highschool', collection 'students')")
    //    sqlContext.sql("SELECT * FROM students WHERE name = 'Torcuato'").show()
    //    sqlContext.sql("DROP TABLE students")


  }

}




