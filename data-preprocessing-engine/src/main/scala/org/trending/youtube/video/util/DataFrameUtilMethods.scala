package org.trending.youtube.video.util

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit}

object DataFrameUtilMethods {

  def getFileName(spark: SparkSession): String = {

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

  def concatDataFrameColumns(colName: String,
                             col1: String,
                             col2: String,
                             literal: String,
                             df: DataFrame): DataFrame = {

    df.withColumn(colName = colName,
      functions.concat(
        col(col1),
        lit("_"),
        col(col2)
      )
    )
  }


}
