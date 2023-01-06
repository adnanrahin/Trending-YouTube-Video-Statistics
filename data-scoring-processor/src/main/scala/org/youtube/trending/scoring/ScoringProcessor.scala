package org.youtube.trending.scoring

import org.apache.spark.rdd.RDD
import org.youtube.trending.schemas.VideoInfoSchema

object ScoringProcessor {

  def findAllTotalViewsByChannelTitle(videoInfoDataRDD: RDD[VideoInfoSchema]): RDD[(String, String)] = {

    val channelViewsRDD: RDD[(String, String)] =
      videoInfoDataRDD
        .map {
          attribute => (attribute.channelTitle, attribute.views.toLong)
        }
        .groupBy(_._1)
        .map {
          attribute => (attribute._1, attribute._2.foldLeft(0L)(_ + _._2).toString)
        }
        .sortBy(_._2)

    channelViewsRDD
  }

}
