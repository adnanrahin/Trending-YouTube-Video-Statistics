package org.trending.youtube.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, input_file_name}
import org.trending.youtube.video.util.{DataFrameUtilMethods, DataWriter, JsonParser}

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

    val countryCodeDF = categoryIdFlattenDF
      .withColumn("country_code",
        callUDF(DataFrameUtilMethods.getFileName(spark), input_file_name()))

    val countryCategoryCode =
      DataFrameUtilMethods
        .concatDataFrameColumns(
          "country_category_code",
          "country_code",
          "items_id",
          "_",
          countryCodeDF
        )

    DataWriter
      .dataWriter(
        countryCategoryCode,
        "data-set/trending_youtube_video_statistics_dataset/",
        "video_category")

  }

}
