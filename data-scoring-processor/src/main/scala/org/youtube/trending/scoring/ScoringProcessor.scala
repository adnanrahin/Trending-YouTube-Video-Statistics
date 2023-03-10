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


  private def findAllMostViewsByDates(videoInfoDataRDD: RDD[VideoInfoSchema]): RDD[Row] = {

    val viewsByDates: RDD[Row] =
      videoInfoDataRDD
        .groupBy(_.trendingDate)
        .map {
          row =>
            val mostView: Long = row._2
              .map(k => k.views).max
            Row(row._1, mostView)
        }
    viewsByDates.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def findAllMostViewsByDatesToDF(viewsByDates: RDD[VideoInfoSchema], spark: SparkSession): DataFrame = {
    val viewsByDatesRDD: RDD[Row] = findAllMostViewsByDates(viewsByDates)
    spark
      .createDataFrame(viewsByDatesRDD, SchemaDefinition.mostViewByDateSchema)

  }


}
