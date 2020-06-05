package com.mayank.spark

object Fibonacci {


    def main(args: Array[String]): Unit = {
      var x1: Int = 0;
      var x2: Int = 1;
      var temp = 0
      for(i <- 1 to 10){
        println(x1 + x2)
        temp = x1
        x1 = x2
        x2 = temp + x2
      }
    }
}
