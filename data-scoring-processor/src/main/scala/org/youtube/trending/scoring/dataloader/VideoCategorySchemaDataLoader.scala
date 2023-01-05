package org.youtube.trending.scoring.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.schemas.VideoCategoryIdSchema

class VideoCategorySchemaDataLoader(spark: SparkSession, inputPath: String) extends DataLoader {

  def loadVideoCategoryData(): RDD[VideoCategoryIdSchema] = {

    videoCategoryIdSchemaLoader(spark = spark, inputPath = inputPath)

  }

}
