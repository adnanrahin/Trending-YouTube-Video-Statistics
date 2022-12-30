package org.trending.youtube.video

import org.apache.spark.sql.functions.{callUDF, col, input_file_name, lit}
import org.apache.spark.sql.{SparkSession, functions}
import org.trending.youtube.video.util.{DataWriter, GetFileName, JsonParser}

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
        callUDF(GetFileName.getFileName(spark), input_file_name()))

    val countryCategoryCode =
      countryCodeDF.withColumn("country_category_code",
        functions.concat(
          col("country_code"),
          lit("_"),
          col("items_id")
        )
      )

    DataWriter
      .dataWriter(
        countryCategoryCode,
        "data-set/trending_youtube_video_statistics_dataset/",
        "video_category")

  }

}
