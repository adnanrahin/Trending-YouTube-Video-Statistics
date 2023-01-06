package org.youtube.trending.scoring

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.transformer.{VideoCategorySchemaDataTransformer, VideoInfoSchemaDataTransformer}
import org.youtube.trending.scoring.schemas.{VideoCategoryIdSchema, VideoInfoSchema}

object VideoScoringProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("VideoScoringProcessor")
        .master("local[*]")
        .getOrCreate()

    val videoInfoInputPath = args(0)
    val videoCategoryPath = args(1)

    val videoInfoSchemaDataLoader: VideoInfoSchemaDataTransformer =
      new VideoInfoSchemaDataTransformer(spark = spark, inputPath = videoInfoInputPath)

    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      videoInfoSchemaDataLoader.loadVideoInfoData()

    val videoCategorySchemaDataLoader: VideoCategorySchemaDataTransformer =
      new VideoCategorySchemaDataTransformer(spark = spark, inputPath = videoCategoryPath)

    val videoCategoryIdDataRDD: RDD[VideoCategoryIdSchema] =
      videoCategorySchemaDataLoader.loadVideoCategoryData()

  }

}
