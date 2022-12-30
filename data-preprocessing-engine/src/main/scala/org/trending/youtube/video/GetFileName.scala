package org.trending.youtube.video

import org.apache.spark.sql.SparkSession

object GetFileName {

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


}
