package org.trending.youtube.video

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, concat, input_file_name, lit}

object TrendVideoDataPreProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("DataFrameApp")
        .config("spark.master", "local[*]")
        .getOrCreate()

    val csvFileDF = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("data-set/trending_youtube_video_statistics_dataset/videos_info/*.csv")

    spark
      .udf
      .register("get_file_name",
        (path: String) => path.split("/")
          .last
          .split("\\.")
          .head
          .substring(0, 2))

    println(csvFileDF.count())

    val countryCodeDF = csvFileDF.withColumn("country_code", callUDF("get_file_name", input_file_name()))

    val country_cate_gory_code =
      countryCodeDF.withColumn("country_category_code",
        concat(
          col("country_code"),
          lit("_"),
          col("category_id")
        )
      )

    println(country_cate_gory_code.count())

    country_cate_gory_code
      .write
      .mode(SaveMode.Overwrite)
      .parquet("data-set/trending_youtube_video_statistics_dataset/videos_info_filter")

  }

}
