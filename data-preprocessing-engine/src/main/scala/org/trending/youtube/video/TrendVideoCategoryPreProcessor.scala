package org.trending.youtube.video

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, input_file_name}
import org.trending.youtube.video.util.{Constant, DataFrameUtilMethods, DataWriter, JsonParser}

object TrendVideoCategoryPreProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("TrendVideoCategoryPreProcessor")
        .master("local[*]")
        .getOrCreate()

    val jsonFileDF = spark.read.option("multiline", "true")
      .json(Constant.VIDEO_CATEGORY_INPUT)

    val categoryIdFlattenDF = JsonParser.flattenDF(jsonFileDF)

    val countryCodeDF = categoryIdFlattenDF
      .withColumn("country_code",
        callUDF(DataFrameUtilMethods.getFileName(spark), input_file_name()))

    val countryCategoryCode =
      DataFrameUtilMethods
        .concatDataFrameColumns(
          Constant.CATEGORY_COUNTRY_CODE,
          Constant.COUNTRY_CODE,
          Constant.ITEMS_ID,
          "_",
          countryCodeDF
        )

    DataWriter
      .dataWriter(
        countryCategoryCode,
        Constant.VIDEO_CATEGORY_OUTPUT,
        "video_category")

  }

}
