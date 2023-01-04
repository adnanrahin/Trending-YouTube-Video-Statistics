package org.youtube.trending.scoring

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.schemas.VideoInfoSchema

object VideoScoringProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("VideoScoringProcessor")
        .master("local[*]")
        .getOrCreate()

    val inputPath = "/Users/adnanrahin/source-code/scala/big-data/Trending-YouTube-Video-Statistics/data-set/trending_youtube_video_statistics_dataset/videos_info_filter/*"

    val VideoInfoDataRDD: RDD[VideoInfoSchema] =
      spark.sparkContext.textFile(inputPath)
        .map(row => row.split("\t", -1))
        .map(
          col => {
            val videoID: String = col(0)
            val trendingDate: String = col(1)
            val title: String = col(2)
            val categoryId: String = col(3)
            val publishTime: String = col(4)
            val views: Long = col(5).toLong
            val likes: Long = col(6).toLong
            val dislikes: Long = col(7).toLong
            val commentCount: Long = col(8).toLong
            val thumbnailLink: String = col(9)

            val commentsDisable: Boolean = if (
              col(10)
                .equalsIgnoreCase("False")
            ) false
            else
              true

            val ratingsDisable: Boolean = if (
              col(11)
                .equalsIgnoreCase("False")
            ) false
            else
              true

            val videoErrorOrRemoved: Boolean = if (
              col(12)
                .equalsIgnoreCase("False")
            ) false
            else
              true

            val description: String = col(13)
            val countryCode: String = col(13)
            val countryCategoryCode: String = col(13)

            VideoInfoSchema(
              videoID: String,
              trendingDate: String,
              title: String,
              categoryId: String,
              publishTime: String,
              views: Long,
              likes: Long,
              dislikes: Long,
              commentCount: Long,
              thumbnailLink: String,
              commentsDisable: Boolean,
              ratingsDisable: Boolean,
              videoErrorOrRemoved: Boolean,
              description: String,
              countryCode: String,
              countryCategoryCode: String
            )
          }
        )

    VideoInfoDataRDD.foreach(f => println)

  }

}
