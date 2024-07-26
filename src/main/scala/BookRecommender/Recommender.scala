package BookRecommender

import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, count, desc, explode, lit, row_number}
import org.apache.spark.sql.{DataFrame, functions}

class Recommender(val booksData :DataFrame) {

  val categories = booksData.select(col("categories")).distinct()

  def recommendBooks(): DataFrame = {

    println("Recommend books")
   // booksData.filter(col("review_score").isNotNull)
    val w: WindowSpec = Window.partitionBy(col("title"), col("authors"), col("categories")).orderBy(desc("review_score"))
    rankCalc(w)
  }

  def recommendAuthors(): DataFrame = {

    println("Recommend authors")
    val w = Window.partitionBy(col("authors"),col("categories")).orderBy(desc("review_score"))
    rankCalc(w)
  }

  def recommendPublishers(): DataFrame = {

    println("Recommend publishers")
    val w = Window.partitionBy(col("publishers"),col("categories")).orderBy(desc("review_score"))
    rankCalc(w)
  }

  def recommendCategories(): DataFrame = {

    println("Recommend categories")
    val w = Window.partitionBy(col("categories")).orderBy(desc("review_score"))
    rankCalc(w)
  }


  def rankAgg(w:WindowSpec): DataFrame = {
    booksData
      .groupBy("title", "authors", "categories")
      .agg(functions.avg("review_score").as("avg_score"))
      .orderBy(desc("avg_score"))
  }

  def rankCalc(w:WindowSpec): DataFrame = {
    booksData
      .withColumn("countRevs", count("review_score").over(w))
      .withColumn("rn", row_number.over(w)).where(col("rn") < 5 )
      .select("title", "authors", "categories", "review_score", "countRevs","rn")
  }

}
