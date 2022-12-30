package org.trending.youtube.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.trending.youtube.video.util.DataWriter

object TrendVideoDataPreProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("TrendVideoDataPreProcessor")
        .master("local[*]")
        .getOrCreate()

    val csvFileDF = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("data-set/trending_youtube_video_statistics_dataset/videos_info/*.csv")

    val countryCodeDF = csvFileDF
      .withColumn("country_code",
        callUDF(GetFileName.getFileName(spark), input_file_name()))

    val countryCategoryCode =
      countryCodeDF.withColumn("country_category_code",
        concat(
          col("country_code"),
          lit("_"),
          col("category_id")
        )
      )

    DataWriter
      .dataWriter(
        countryCategoryCode,
        "data-set/trending_youtube_video_statistics_dataset/",
        "videos_info_filter")

  }

}
