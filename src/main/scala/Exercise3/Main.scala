package Exercise3
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Movie Stats")
      .master("local")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("movieId", IntegerType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("genres", StringType, nullable = true)
    ))

    val filePath = "src/main/scala/Exercise3/movies.csv"
    val moviesDF = spark.read
      .option("header" , "false")
      .schema(schema)
      .csv(filePath)

    // 3.1
    val genresCount = moviesDF
      .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
      .groupBy("genre")
      .count()
      .orderBy("genre")

    println("Movies per Genre:")
    genresCount.show()

    // 3.2
    val moviesPerYear = moviesDF
      .withColumn("year", F.regexp_extract(F.col("title"), "\\((\\d{4})\\)", 1)) // take year
      .filter(F.col("year") =!= "") // filter lines without colName yaear
      .groupBy("year")
      .count()
      .orderBy(F.desc("count"))

    println("Top 10 Years with Most Movies:")
    moviesPerYear.show(10)

    // 3.3
    val frequentWords = moviesDF
      .withColumn("lower_title", F.lower(F.col("title"))) // filtering the titles to lowercase
      .withColumn("words", F.explode(F.split(F.col("lower_title"), "\\s+")))
      .filter(F.length(F.col("words")) >= 4) // filtering words less than 4 chars
      .filter(F.col("words").rlike("^[a-zA-Z]+$")) // include only letter, otherwise years movies were released are among the most common words
      .groupBy("words")
      .count()
      .filter(F.col("count") >= 10) //at least 10 times
      .orderBy(F.desc("count"))



    println("Frequent Words in Titles:")
    frequentWords.show()

    // save to txt
    val outputPath33 = "src/main/scala/Exercise3/results_movies"
    frequentWords
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath33)

    val outputPath31 = "src/main/scala/Exercise3/results_movies_genre"
    genresCount
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath31)

    val outputPath32 = "src/main/scala/Exercise3/results_movies_year"
    moviesPerYear
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath32)

    spark.stop()
  }
}

