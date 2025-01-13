package Exercise1
import org.apache.spark.{SparkConf, SparkContext}

object Main{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Length Average").setMaster("local")
    val sc = new SparkContext(conf)

    // input sherlock holmes books
    val inputFile = "src/main/scala/Exercise1/SherlockHolmes.txt"
    val textFile = sc.textFile(inputFile)

    // clean text data
    val words = textFile
      .flatMap(line => line.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase.split("\\s+"))
      .filter(word =>word.nonEmpty && word.head.isLetter) // if the word starts with number, remove it

    val letterWordLengths = words
      .map(word => (word.head, word.length))
      .groupByKey()
      .mapValues(lengths => lengths.sum.toDouble / lengths.size) // find average length

    // sorting in ascending order
    val sortedResults = letterWordLengths.sortBy({ case (_, avgLength) => avgLength }, ascending = false)

    // results
    sortedResults.collect().foreach { case (letter, avgLength) =>
      println(f"$letter $avgLength%.2f")
    }

    sc.stop()
  }
}
