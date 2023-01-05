package org.youtube.trending.scoring

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.dataloader.VideoInfoSchemaDataLoader
import org.youtube.trending.scoring.schemas.VideoInfoSchema

object VideoScoringProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("VideoScoringProcessor")
        .master("local[*]")
        .getOrCreate()

    val videoInfoInputPath = args(0)

    val videoInfoSchemaDataLoader: VideoInfoSchemaDataLoader =
      new VideoInfoSchemaDataLoader(spark = spark, inputPath = videoInfoInputPath)

    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      videoInfoSchemaDataLoader.loadVideoInfoData()

    videoInfoDataRDD.foreach(f => println(f.ratingsDisable))

  }

}
