package BookRecommender

import org.apache.spark.sql.{DataFrame, SparkSession}

object Books {


  val spark: SparkSession = SparkSession.builder()
    .appName("iqvia-spark")
    .master("local")
    .getOrCreate()


  def booksData(booksDataPath: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(booksDataPath)
      .toDF
      .select("title", "description", "authors", "publisher", "categories", "ratingscount")
  }


  def booksRating(booksRatingPath: String):DataFrame = {
    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(booksRatingPath)
      .toDF
      .select("Id","Title","user_Id","review_helpfulness","review_score","review_summary","review_text")
  }
}
