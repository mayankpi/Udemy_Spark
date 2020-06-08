package com.mayank.spark


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurrences(line: String): (Int, Int) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile("C:\\Users\\Mayank Maurya\\Downloads\\Udemy Courses\\Apache spark using scala\\SparkScala3\\marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)

    // Load up the superhero co-appearance data
    val lines = sc.textFile("C:\\Users\\Mayank Maurya\\Downloads\\Udemy Courses\\Apache spark using scala\\SparkScala3\\marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurrences)

    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )

    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )

    val sorted = flipped.sortByKey(ascending = false)


    // Find the max # of connections
    val mostPopular = flipped.max()

    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

    // Print out our answer!
    val topFew = sorted.take(10)

    for( each <- topFew){
      val name = namesRdd.lookup(each._2)(0)
      println(s"$name is a popular superhero with ${each._1} co-appearences")
    }
    //println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
  }

}

