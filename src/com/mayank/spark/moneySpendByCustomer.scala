package com.mayank.spark

import org.apache.log4j._
import org.apache.spark._

object moneySpendByCustomer {

  def parseLine(line: String) : (Int, Float) = {
    val fields = line.split(",")
    val customer = fields(0).toInt
    val moneySpent = fields(2).toFloat
    (customer, moneySpent)
  }

  def main(args: Array[String]): Unit = {

    //set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //creating sparkcontext using every core of the local machine
    val sc = new SparkContext("local[*]", "com.mayank.spark.moneySpendByCustomer" )

    //read each line of the data
    val lines = sc.textFile("C:\\Users\\Mayank Maurya\\Downloads\\Udemy Courses\\Apache spark using scala\\SparkScala3\\customer-orders.csv")

    //convert to (customerID, PriceSpent) tuple
    val parsedLines = lines.map(parseLine)



    //reduce the customeID and adding the amount spent
    val amountSpent = parsedLines.reduceByKey((x,y) => x+y)

    //making price as key
    val priceAsKey = amountSpent.map(x => (x._2, x._1))

    val results = priceAsKey.collect()

    for(result <- results.sorted){
      val totalAmount = result._1
      val customerID = result._2
      val formattedAmount = f"$totalAmount%.2f"
      println(s"$formattedAmount was spent by:  $customerID")
    }
  }
}
