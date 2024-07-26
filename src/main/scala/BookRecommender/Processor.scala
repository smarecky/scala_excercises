package BookRecommender

import org.apache.spark.sql.DataFrame

class Processor(val booksData :DataFrame , val booksRating :DataFrame) {

  def processReviews(): DataFrame = {
    booksData.join(booksRating, booksData("title") === booksRating("title1"))
    //.select("title", "description", "authors", "publisher", "categories", "ratingscount", "Id", "user_Id", "review_helpfulness", "review_score", "review_summary", "review_text")

  }
}
