package org.youtube.trending.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.youtube.trending.schemas.{VideoCategoryIdSchema, VideoInfoSchema}
import org.youtube.trending.util.UtilMethods

trait DataTransformer {

  def videoInfoTransformer(spark: SparkSession, inputPath: String): RDD[VideoInfoSchema] = {

    import spark.implicits._
    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      spark.read.parquet(inputPath)
        .filter(row => !row.toString().startsWith("\n"))
        .map(

          videoInfoAttribute => {

            val videoID: String = if (videoInfoAttribute(0) == null) "empty" else videoInfoAttribute(0).toString.replaceAll("\n", " ").trim
            val trendingDate: String = if (videoInfoAttribute(1) == null) "empty" else videoInfoAttribute(1).toString.replaceAll("\n", " ").trim
            val title: String = if (videoInfoAttribute(2) == null) "empty" else videoInfoAttribute(2).toString.replaceAll("\n", " ").trim
            val channelTitle: String = if (videoInfoAttribute(3) == null) "empty" else videoInfoAttribute(3).toString.replaceAll("\n", " ").trim
            val categoryId: String = if (videoInfoAttribute(4) == null) "empty" else videoInfoAttribute(4).toString.replaceAll("\n", " ").trim
            val publishTime: String = if (videoInfoAttribute(5) == null) "empty" else videoInfoAttribute(5).toString.replaceAll("\n", " ").trim
            val tags: String = if (videoInfoAttribute(6) == null) "empty" else videoInfoAttribute(6).toString.replaceAll("\n", " ").trim
            val views: Long = if (videoInfoAttribute(7) != null && UtilMethods.isNumeric(videoInfoAttribute(7).toString)) videoInfoAttribute(7).toString.replaceAll("[^0-9]", "").toLong else 0L
            val likes: Long = if (videoInfoAttribute(8) != null && UtilMethods.isNumeric(videoInfoAttribute(8).toString)) videoInfoAttribute(8).toString.replaceAll("[^0-9]", "").toLong else 0L
            val dislikes: Long = if (videoInfoAttribute(9) != null && UtilMethods.isNumeric(videoInfoAttribute(9).toString)) videoInfoAttribute(9).toString.replaceAll("[^0-9]", "").toLong else 0L
            val commentCount: Long = if (videoInfoAttribute(10) != null && UtilMethods.isNumeric(videoInfoAttribute(10).toString)) videoInfoAttribute(10).toString.replaceAll("[^0-9]", "").toLong else 0L
            val thumbnailLink: String = if (videoInfoAttribute(11) == null) "empty" else videoInfoAttribute(11).toString.replaceAll("\n", " ").trim
            val commentsDisable: Boolean = if (videoInfoAttribute(12) != null && videoInfoAttribute(12).toString.trim.equalsIgnoreCase("False")) false else true
            val ratingsDisable: Boolean = if (videoInfoAttribute(13) != null && videoInfoAttribute(13).toString.trim.equalsIgnoreCase("False")) false else true
            val videoErrorOrRemoved: Boolean = if (videoInfoAttribute(14) != null && videoInfoAttribute(14).toString.trim.equalsIgnoreCase("False")) false else true
            val description: String = if (videoInfoAttribute(15) == null) "empty" else videoInfoAttribute(15).toString.replaceAll("\n", " ").trim
            val countryCode: String = if (videoInfoAttribute(16) == null) "empty" else videoInfoAttribute(16).toString.replaceAll("\n", " ").trim
            val countryCategoryCode: String = if (videoInfoAttribute(17) == null) "empty" else videoInfoAttribute(17).toString.replaceAll("\n", " ").trim

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

    videoInfoDataRDD.persist(StorageLevel.MEMORY_AND_DISK)

  }

  def videoCategoryIdTransformer(spark: SparkSession, inputPath: String): RDD[VideoCategoryIdSchema] = {

    import spark.implicits._
    val videoCategoryIdRDD: RDD[VideoCategoryIdSchema] =
      spark.read.parquet(inputPath)
        .map(

          videoCategoryIdAttribute => {

            val etag: String = if (videoCategoryIdAttribute(0) == null) "empty" else videoCategoryIdAttribute(0).toString
            val kind: String = if (videoCategoryIdAttribute(1) == null) "empty" else videoCategoryIdAttribute(1).toString
            val itemsEtag: String = if (videoCategoryIdAttribute(2) == null) "empty" else videoCategoryIdAttribute(2).toString
            val itemsId: String = if (videoCategoryIdAttribute(3) == null) "empty" else videoCategoryIdAttribute(3).toString
            val itemsKind: String = if (videoCategoryIdAttribute(4) == null) "empty" else videoCategoryIdAttribute(4).toString
            val itemSnippetsAssignable: Boolean = if (videoCategoryIdAttribute(5) != null && videoCategoryIdAttribute(5).toString.trim.equalsIgnoreCase("False")) false else true
            val itemSnippetsChannelId: String = if (videoCategoryIdAttribute(6) == null) "empty" else videoCategoryIdAttribute(6).toString
            val itemSnippetsTable: String = if (videoCategoryIdAttribute(7) == null) "empty" else videoCategoryIdAttribute(7).toString
            val countryCode: String = if (videoCategoryIdAttribute(8) == null) "empty" else videoCategoryIdAttribute(8).toString
            val countryCategoryCode: String = if (videoCategoryIdAttribute(9) == null) "empty" else videoCategoryIdAttribute(9).toString


            VideoCategoryIdSchema(
              etag: String,
              kind: String,
              itemsEtag: String,
              itemsId: String,
              itemsKind: String,
              itemSnippetsAssignable: Boolean,
              itemSnippetsChannelId: String,
              itemSnippetsTable: String,
              countryCode: String,
              countryCategoryCode: String
            )
          }
        ).rdd

    videoCategoryIdRDD.persist(StorageLevel.MEMORY_AND_DISK)
  }

}
