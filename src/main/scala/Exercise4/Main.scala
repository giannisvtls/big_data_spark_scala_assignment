package Exercise4

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Graph Analysis")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val edges = sc.textFile("src/main/scala/Exercise4/web-Stanford.txt")
      .filter(!_.startsWith("#")) // Ignore comments
      .map(line => {
        val parts = line.split("\\s+")
        (parts(0).toInt, parts(1).toInt)
      })

    // Calculate in-degree and out-degree for each node

    // Calculate out-degrees
    val outDegrees = edges.map(edge => (edge._1, 1))
      .reduceByKey(_ + _)

    // Calculate in-degrees
    val inDegrees = edges.map(edge => (edge._2, 1))
      .reduceByKey(_ + _)

    // Get top 10 nodes by out-degree
    println("Top 10 nodes by out-degree:")
    outDegrees.sortBy(_._2, ascending = false)
      .take(10)
      .foreach(node => println(s"Node ${node._1}: ${node._2} outgoing edges"))

    // Get top 10 nodes by in-degree
    println("\nTop 10 nodes by in-degree:")
    inDegrees.sortBy(_._2, ascending = false)
      .take(10)
      .foreach(node => println(s"Node ${node._1}: ${node._2} incoming edges"))

    // Calculate total degree for each node

    // Combine in-degrees and out-degrees
    val allDegrees = outDegrees.fullOuterJoin(inDegrees)
      .mapValues(pair => {
        val outDeg = pair._1.getOrElse(0)
        val inDeg = pair._2.getOrElse(0)
        outDeg + inDeg
      })

    // Calculate average degree
    val totalNodes = allDegrees.count()
    val totalDegrees = allDegrees.values.sum()
    val averageDegree = totalDegrees.toDouble / totalNodes

    // Count nodes with degree >= average
    val nodesAboveAverage = allDegrees
      .filter(_._2 >= averageDegree)
      .count()

    println(s"\nAnalysis Results:")
    println(s"Total nodes: $totalNodes")
    println(s"Average degree: $averageDegree")
    println(s"Nodes with degree >= average: $nodesAboveAverage")

    sc.stop()
  }
}
