package org.youtube.trending.scoring

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.schemas.VideoInfoSchema
import org.youtube.trending.scoring.util.UtilMethods

object VideoScoringProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("VideoScoringProcessor")
        .master("local[*]")
        .getOrCreate()

    val inputPath = "/Users/adnanrahin/source-code/scala/big-data/Trending-YouTube-Video-Statistics/data-set/trending_youtube_video_statistics_dataset/videos_info_filter/*"

    import spark.implicits._
    /*
        videoInfoDF.show(20)

        videoInfoDF.map(attributes => "Name: " + attributes(0)).show(20)
    */

    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      spark.read.parquet(inputPath)
        .map(
          col => {
            val videoID: String = if
            (col(0).toString != null || col(0).toString.nonEmpty)
              col(0).toString
            else "empty"

            val trendingDate: String = if
            (col(1).toString != null || col(1).toString.nonEmpty)
              col(1).toString
            else "empty"

            val title: String = if
            (col(2).toString != null || col(2).toString.nonEmpty)
              col(2).toString
            else "empty"

            val categoryId: String = if
            (col(3).toString != null || col(3).toString.nonEmpty)
              col(3).toString
            else "empty"

            val publishTime: String = if
            (col(4).toString != null || col(4).toString.nonEmpty)
              col(4).toString
            else "empty"

            val views: Long = if (
              UtilMethods
                .isNumeric(col(5).toString)
            ) col(5).toString.toLong
            else 0L

            val likes: Long = if (
              UtilMethods
                .isNumeric(col(6).toString)
            ) col(6).toString.toLong
            else 0L

            val dislikes: Long = if (
              UtilMethods
                .isNumeric(col(7).toString)
            ) col(7).toString.toLong
            else 0L

            val commentCount: Long = if (
              UtilMethods
                .isNumeric(col(8).toString)
            ) col(8).toString.toLong
            else 0L

            val thumbnailLink: String = if
            (col(9).toString != null || col(9).toString.nonEmpty)
              col(9).toString
            else "empty"

            val commentsDisable: Boolean = if (
              col(10).toString
                .equalsIgnoreCase("False")
            ) false
            else
              true

            val ratingsDisable: Boolean = if (
              col(11).toString
                .equalsIgnoreCase("False")
            ) false
            else
              true

            val videoErrorOrRemoved: Boolean = if (
              col(12).toString
                .equalsIgnoreCase("False")
            ) false
            else
              true

            val description: String = if
            (col(13).toString != null || col(13).toString.nonEmpty)
              col(13).toString
            else "empty"

            val countryCode: String = if
            (col(14).toString != null || col(14).toString.nonEmpty)
              col(14).toString
            else "empty"

            val countryCategoryCode: String = if
            (col(15).toString != null || col(15).toString.nonEmpty)
              col(15).toString
            else "empty"

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
        ).rdd

    videoInfoDataRDD.foreach(r => println(r.videoID))

  }

}
