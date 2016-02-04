package com.oreilly.learningsparkexamples.scala

import java.sql.DriverManager
/**
 * Created by user on 2016/1/28.
 */
object JDBCofSparkSQL {
  def main(args: Array[String]) {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://chenhuimin03.localdomain:10000", "", "")
    try {
      val statement = conn.createStatement()
      val rs = statement.executeQuery("select ordernumber,amount from tbStockDetail  where amount>3000")
      while (rs.next()) {
        val ordernumber = rs.getString("ordernumber")
        val amount = rs.getString("amount")
        println("ordernumber = %s, amount = %s".format(ordernumber, amount))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    conn.close
  }

}
