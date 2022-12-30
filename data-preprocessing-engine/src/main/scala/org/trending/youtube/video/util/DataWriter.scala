package org.trending.youtube.video.util

import org.apache.spark.sql.{DataFrame, SaveMode}

object DataWriter {
  final def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + "/" + directoryName

    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationDirectory)
  }
}
