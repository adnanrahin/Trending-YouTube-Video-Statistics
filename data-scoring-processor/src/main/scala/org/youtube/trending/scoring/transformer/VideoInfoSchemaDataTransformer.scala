package org.youtube.trending.scoring.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.schemas.VideoInfoSchema

class VideoInfoSchemaDataTransformer(spark: SparkSession, inputPath: String) extends DataTransformer {

  def loadVideoInfoData(): RDD[VideoInfoSchema] = {

    videoInfoTransformer(spark = spark, inputPath = inputPath)

  }

}
