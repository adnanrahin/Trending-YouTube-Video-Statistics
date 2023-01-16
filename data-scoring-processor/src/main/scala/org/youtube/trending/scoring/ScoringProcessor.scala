package org.youtube.trending.scoring

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.youtube.trending.schemas.{SchemaDefinition, VideoInfoSchema}


object ScoringProcessor {

  private def findAllTotalViewsByChannelTitle(videoInfoDataRDD: RDD[VideoInfoSchema]): RDD[Row] = {

    val channelViewsRDD: RDD[Row] =
      videoInfoDataRDD
        .map {
          attribute => (attribute.channelTitle, attribute.views.toLong)
        }
        .groupBy(_._1)
        .map {
          attribute => Row(attribute._1, attribute._2.foldLeft(0L)(_ + _._2))
        }

    channelViewsRDD.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def findAllTotalViewsByChannelTitleToDf(channelViewsRDD: RDD[VideoInfoSchema], spark: SparkSession): DataFrame = {
    val channelViews: RDD[Row] = findAllTotalViewsByChannelTitle(channelViewsRDD)
    spark
      .createDataFrame(channelViews, SchemaDefinition.viewCountSchema)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }


}
