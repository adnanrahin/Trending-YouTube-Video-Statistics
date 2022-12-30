package org.trending.youtube.video

import org.apache.spark.sql.functions.{callUDF, col, input_file_name, lit}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.trending.youtube.video.util.JsonParser

object TrendVideoCategoryPreProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("TrendVideoCategoryPreProcessor")
        .master("local[*]")
        .getOrCreate()

    val jsonFileDF = spark.read.option("multiline", "true")
      .json("data-set/trending_youtube_video_statistics_dataset/category_ids/*.json")

    val categoryIdFlattenDF = JsonParser.flattenDF(jsonFileDF)

    //    spark
    //      .udf
    //      .register("get_file_name",
    //        (path: String) => path.split("/")
    //          .last
    //          .split("\\.")
    //          .head
    //          .substring(0, 2))


    val countryCodeDF = categoryIdFlattenDF
      .withColumn("country_code",
        callUDF(getFileName(spark), input_file_name()))

    val countryCategoryCode =
      countryCodeDF.withColumn("country_category_code",
        functions.concat(
          col("country_code"),
          lit("_"),
          col("items_id")
        )
      )

    countryCategoryCode
      .write
      .mode(SaveMode.Overwrite)
      .parquet("data-set/trending_youtube_video_statistics_dataset/video_category")

  }

  private def getFileName(spark: SparkSession): String = {

    val fileName: String = "getFileName"

    spark
      .udf
      .register("getFileName",
        (path: String) => path.split("/")
          .last
          .split("\\.")
          .head
          .substring(0, 2))

    fileName

  }

}
