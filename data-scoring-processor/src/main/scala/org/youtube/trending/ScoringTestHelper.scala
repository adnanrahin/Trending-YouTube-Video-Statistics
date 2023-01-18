package org.youtube.trending

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.schemas.VideoInfoSchema
import org.youtube.trending.transformer.VideoInfoSchemaDataTransformer

object ScoringTestHelper {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("VideoScoringProcessor")
        .master("local[*]")
        .getOrCreate()

    val videoInfoInputPath = args(0)
    val videoCategoryPath = args(1)
    val outputDirectory = args(2)

    val videoInfoSchemaDataLoader: VideoInfoSchemaDataTransformer =
      new VideoInfoSchemaDataTransformer(spark = spark, inputPath = videoInfoInputPath)

    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      videoInfoSchemaDataLoader.loadVideoInfoData()


  }

}
