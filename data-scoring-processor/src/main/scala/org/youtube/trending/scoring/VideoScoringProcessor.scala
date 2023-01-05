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

    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      spark.read.parquet(inputPath)
        .map(
          col => {

            val videoID: String = if (col(0) == null) "empty" else col(0).toString
            val trendingDate: String = if (col(1) == null) "empty" else col(1).toString
            val title: String = if (col(2) == null) "empty" else col(2).toString
            val channelTitle: String = if (col(3) == null) "empty" else col(3).toString
            val categoryId: String = if (col(4) == null) "empty" else col(4).toString
            val publishTime: String = if (col(5) == null) "empty" else col(5).toString
            val tags: String = if (col(6) == null) "empty" else col(6).toString
            val views: Long = if (col(7) != null && UtilMethods.isNumeric(col(7).toString)) col(7).toString.replaceAll("[^0-9]", "").toLong else 0L
            val likes: Long = if (col(8) != null && UtilMethods.isNumeric(col(8).toString)) col(8).toString.replaceAll("[^0-9]", "").toLong else 0L
            val dislikes: Long = if (col(9) != null && UtilMethods.isNumeric(col(9).toString)) col(9).toString.replaceAll("[^0-9]", "").toLong else 0L
            val commentCount: Long = if (col(10) != null && UtilMethods.isNumeric(col(10).toString)) col(10).toString.replaceAll("[^0-9]", "").toLong else 0L
            val thumbnailLink: String = if (col(11) == null) "empty" else col(11).toString
            val commentsDisable: Boolean = if (col(12) != null && col(12).toString.trim.equalsIgnoreCase("False")) false else true
            val ratingsDisable: Boolean = if (col(13) != null && col(13).toString.trim.equalsIgnoreCase("False")) false else true
            val videoErrorOrRemoved: Boolean = if (col(14) != null && col(14).toString.trim.equalsIgnoreCase("False")) false else true
            val description: String = if (col(15) == null) "empty" else col(15).toString
            val countryCode: String = if (col(16) == null) "empty" else col(16).toString
            val countryCategoryCode: String = if (col(17) == null) "empty" else col(17).toString

            VideoInfoSchema(
              videoID: String,
              trendingDate: String,
              title: String,
              channelTitle: String,
              categoryId: String,
              publishTime: String,
              tags: String,
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

    videoInfoDataRDD.foreach(r => println(r.videoID + " " + r.thumbnailLink))

  }

}
