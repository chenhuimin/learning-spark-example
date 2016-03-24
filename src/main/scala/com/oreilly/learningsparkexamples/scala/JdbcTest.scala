package com.oreilly.learningsparkexamples.scala

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/3/24 0024.
  */
object JdbcTest {
  def main(args: Array[String]) {
    val props = scala.collection.mutable.Map[String, String]();
    props += ("driver" -> "com.mysql.jdbc.Driver")
    props += ("url" -> "jdbc:mysql://node3.laanto.com:3306/test?user=root&password=123abc..")
    props += ("dbtable" -> "(select * from person) as person")
    props += ("partitionColumn" -> "person_id")
    props += ("lowerBound" -> "0")
    props += ("upperBound" -> "100")
    props += ("numPartitions" -> "2")

    import scala.collection.JavaConverters._
    val master = "spark://node2.laanto.com:7077"
    val conf = new SparkConf().setMaster(master).setAppName("DeepMongodbTest").set("spark.executor.memory", "1024M").set("spark.executor.cores", "3").set("spark.cores.max", "9")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    //val personDf = sqlContext.load("jdbc", props.asJava)
    val personDf = sqlc.read.format("jdbc").options(props.asJava).load()
    personDf.printSchema()
    personDf.show()
    personDf.registerTempTable("person")
  }

}
