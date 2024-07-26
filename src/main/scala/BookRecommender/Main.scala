package BookRecommender

import org.apache.spark
import org.apache.spark.sql.functions.{callUDF, call_udf, col, collect_list, concat, lit}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import BookRecommender.Books.{booksData, booksRating, spark}

object Main {
  def main(args: Array[String]): Unit = {


    val booksDataPath = "/Users/msamborski/Downloads/archive/books_data.csv"

    val booksRatingPath = "/Users/msamborski/Downloads/archive/Books_rating.csv"

    val books = booksData(booksDataPath).withColumnRenamed("Title","title")

    val rating = booksRating(booksRatingPath).withColumnRenamed("Title","title1")

    val reviewPool = new Processor(books, rating)

    val recommender = new Recommender(reviewPool.processReviews())

    reviewPool.processReviews().show()

    recommender.recommendBooks().show()
    recommender.recommendAuthors().show()
    recommender.recommendCategories().show()
    recommender.recommendPublishers().show()

  }
}