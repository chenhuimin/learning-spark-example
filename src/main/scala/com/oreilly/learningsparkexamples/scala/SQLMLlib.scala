package com.oreilly.learningsparkexamples.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by user on 2016/2/2.
 */
object SQLMLlib {
  def main(args: Array[String]) {
    val fileOutPath = args(0)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val sparkConf = new SparkConf().setAppName("SQLMLlib")
    val sc = new SparkContext(sparkConf)
    //sc.addJar(getClass().getProtectionDomain().getCodeSource().getLocation().getFile() + "../learning-spark-example-1.0-SNAPSHOT.jar")
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("SET spark.sql.shuffle.partitions=20")


    val sqldata = hiveContext.sql("select a.locationid, sum(b.qty) totalqty,sum(b.amount) totalamount from tbStock a join tbStockDetail b on a.ordernumber=b.ordernumber group by a.locationid")
    val parsedData = sqldata.map {
      case Row(_, totalqty, totalamount) =>
        val features = Array[Double](totalqty.toString.toDouble,totalamount.toString.toDouble)
        Vectors.dense(features)
    }

    val numClusters = 3
    val numIterations = 20
    val model = KMeans.train(parsedData, numClusters, numIterations)

    val result2 = sqldata.map {
      case Row(locationid, totalqty, totalamount) =>
        val features = Array[Double](totalqty.toString.toDouble, totalamount.toString.toDouble)
        val linevectore = Vectors.dense(features)
        val prediction = model.predict(linevectore)
        locationid + " " + totalqty + " " + totalamount + " " + prediction
    }

    result2.saveAsTextFile(fileOutPath)
    sc.stop()
  }

}
