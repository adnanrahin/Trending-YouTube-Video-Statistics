package org.trending.youtube.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.trending.youtube.video.util.{DataWriter, DataFrameUtilMethods}

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
      .load("/Users/adnanrahin/source-code/scala/big-data/Trending-YouTube-Video-Statistics/data-set/trending_youtube_video_statistics_dataset/videos_info/*.csv")

    val countryCodeDF = csvFileDF
      .withColumn("country_code",
        callUDF(DataFrameUtilMethods.getFileName(spark), input_file_name()))

    val countryCategoryCode =
      DataFrameUtilMethods
        .concatDataFrameColumns(
          "country_category_code",
          "country_code",
          "category_id",
          "_",
          countryCodeDF
        )

    DataWriter
      .dataWriter(
        countryCategoryCode,
        "data-set/trending_youtube_video_statistics_dataset/",
        "videos_info_filter")

  }

}
