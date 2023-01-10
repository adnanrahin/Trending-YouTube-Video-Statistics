package org.youtube.trending

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.util.DataWriter
import org.youtube.trending.schemas.{VideoCategoryIdSchema, VideoInfoSchema}
import org.youtube.trending.scoring.ScoringProcessor
import org.youtube.trending.transformer.{VideoCategorySchemaDataTransformer, VideoInfoSchemaDataTransformer}

object VideoScoringProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("VideoScoringProcessor")
        //.master("spark://spark-master:7077")
        .getOrCreate()

    val videoInfoInputPath = args(0)
    val videoCategoryPath = args(1)
    val outputDirectory = args(2)

    val videoInfoSchemaDataLoader: VideoInfoSchemaDataTransformer =
      new VideoInfoSchemaDataTransformer(spark = spark, inputPath = videoInfoInputPath)

    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      videoInfoSchemaDataLoader.loadVideoInfoData()

    val videoCategorySchemaDataLoader: VideoCategorySchemaDataTransformer =
      new VideoCategorySchemaDataTransformer(spark = spark, inputPath = videoCategoryPath)

    val videoCategoryIdDataRDD: RDD[VideoCategoryIdSchema] =
      videoCategorySchemaDataLoader.loadVideoCategoryData()

    DataWriter.dataWriter(ScoringProcessor.findAllTotalViewsByChannelTitleToDf(videoInfoDataRDD, spark)
      , outputDirectory,
      "channel_total_views_count")

  }

}
