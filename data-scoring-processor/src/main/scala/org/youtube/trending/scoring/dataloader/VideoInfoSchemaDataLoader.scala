package org.youtube.trending.scoring.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.schemas.VideoInfoSchema

class VideoInfoSchemaDataLoader(spark: SparkSession, inputPath: String) extends DataLoader {

  def loadVideoInfoData(): RDD[VideoInfoSchema] = {

    videoInfoSchemaLoader(spark = spark, inputPath = inputPath)

  }

}
