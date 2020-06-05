package com.mayank.spark

object LIstsTest {
  def main(args: Array[String]): Unit = {
    val numList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val divByThree = numList.filter( (x: Int) => (x %3 != 0) )

    println(divByThree)
  }
}
