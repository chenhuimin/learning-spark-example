package com.oreilly.learningsparkexamples.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by user on 2016/1/15.
 */
object NetworkWordCount {

  def main(args: Array[String]) {

    //    val br = new BufferedReader(new FileReader(getClass().getProtectionDomain().getCodeSource().getLocation().getFile() + "input.txt"))
    //    while ((br.readLine()) != null) {
    //      println(br.readLine())
    //    }


        val master = args(0)
        val fileInPath = args(1)
        val fileOutPath = args(2)
        val conf = new SparkConf().setMaster(master).setAppName("NetworkWordCount").set("spark.executor.memory", "512M").set("spark.executor.cores", "1")
        val sc = new SparkContext(conf)

        sc.addJar(getClass().getProtectionDomain().getCodeSource().getLocation().getFile() + "../learning-spark-example-1.0-SNAPSHOT.jar")
        val wordCountRdd = sc.textFile(fileInPath).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        wordCountRdd.saveAsTextFile(fileOutPath)


  }


}
