package org.trending.youtube.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.trending.youtube.video.util.{Constant, DataFrameUtilMethods, DataWriter}

object TrendVideoDataPreProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("TrendVideoDataPreProcessor")
        .master("local[*]")
        .getOrCreate()

    val inputSource: String = Constant.VIDEO_INFO_INPUT

    val csvFileDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(inputSource)

    val countryCodeDF = csvFileDF
      .withColumn(Constant.COUNTRY_CODE,
        callUDF(DataFrameUtilMethods.getFileName(spark), input_file_name()))

    val countryCategoryCode =
      DataFrameUtilMethods
        .concatDataFrameColumns(
          Constant.CATEGORY_COUNTRY_CODE,
          Constant.COUNTRY_CODE,
          Constant.CATEGORY_ID,
          "_",
          countryCodeDF
        )

    val replacedAndFilteredDf = countryCategoryCode
      .withColumn("video_id", when(col("video_id").startsWith("\\n"),
      regexp_replace(col("video_id"), ".*", " "))
      .otherwise(col("video_id")))

    DataWriter
      .dataWriter(
        replacedAndFilteredDf,
        Constant.VIDEO_INFO_OUTPUT,
        "videos_info_filter")

  }

}
