package com.mayank.spark

object ToUppercase {
    def convertToUppercase( x: String, f: String => String ) : String = {
        f(x)
    }

    def main(args: Array[String] ) : Unit = {
        val x: String = "mayank"
        val result = convertToUppercase(x, x => x.toUpperCase())
        println(result)
    }
}
