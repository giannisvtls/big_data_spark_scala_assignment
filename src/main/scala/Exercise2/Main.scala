package Exercise2

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Airline Tweet Analysis")
      .master("local[*]")
      .getOrCreate()

    val tweets = spark.read
      .option("header", "true")
      .csv("src/main/scala/Exercise2/tweets.csv")

    // Top 5 words by sentiment
    def getTopWordsBySentiment(sentiment: String): DataFrame = {
      tweets
        .filter(col("airline_sentiment") === sentiment)
        // Convert text to lowercase and split into words
        .select(explode(split(lower(regexp_replace(col("text"), "[^a-zA-Z\\s]", " ")), "\\s+")).as("word"))
        .filter(length(col("word")) > 0)  // Remove empty strings
        .groupBy("word")
        .count()
        .orderBy(desc("count"))
        .limit(5)
    }

    // Get results for each sentiment
    println("Top 5 words for positive sentiment:")
    getTopWordsBySentiment("positive").show()

    println("Top 5 words for negative sentiment:")
    getTopWordsBySentiment("negative").show()

    println("Top 5 words for neutral sentiment:")
    getTopWordsBySentiment("neutral").show()

    // Main complaint reason by airline
    val mainComplaintsByAirline = tweets
      .filter(col("negativereason_confidence") > 0.5)
      .filter(col("negativereason").isNotNull)
      .groupBy("airline", "negativereason")
      .count()
      .withColumn("rank", dense_rank().over(
        Window.partitionBy("airline")
          .orderBy(desc("count"))
      ))
      .filter(col("rank") === 1)
      .select("airline", "negativereason", "count")

    println("Main complaint reason by airline:")
    mainComplaintsByAirline.show()

    spark.stop()
  }
}
