package org.youtube.trending.scoring.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.schemas.VideoInfoSchema
import org.youtube.trending.scoring.util.UtilMethods

trait DataLoader {

  def videoInfoSchemaLoader(spark: SparkSession, inputPath: String): RDD[VideoInfoSchema] = {

    import spark.implicits._
    val videoInfoDataRDD: RDD[VideoInfoSchema] =
      spark.read.parquet(inputPath)
        .map(

          attribute => {

            val videoID: String = if (attribute(0) == null) "empty" else attribute(0).toString
            val trendingDate: String = if (attribute(1) == null) "empty" else attribute(1).toString
            val title: String = if (attribute(2) == null) "empty" else attribute(2).toString
            val channelTitle: String = if (attribute(3) == null) "empty" else attribute(3).toString
            val categoryId: String = if (attribute(4) == null) "empty" else attribute(4).toString
            val publishTime: String = if (attribute(5) == null) "empty" else attribute(5).toString
            val tags: String = if (attribute(6) == null) "empty" else attribute(6).toString
            val views: Long = if (attribute(7) != null && UtilMethods.isNumeric(attribute(7).toString)) attribute(7).toString.replaceAll("[^0-9]", "").toLong else 0L
            val likes: Long = if (attribute(8) != null && UtilMethods.isNumeric(attribute(8).toString)) attribute(8).toString.replaceAll("[^0-9]", "").toLong else 0L
            val dislikes: Long = if (attribute(9) != null && UtilMethods.isNumeric(attribute(9).toString)) attribute(9).toString.replaceAll("[^0-9]", "").toLong else 0L
            val commentCount: Long = if (attribute(10) != null && UtilMethods.isNumeric(attribute(10).toString)) attribute(10).toString.replaceAll("[^0-9]", "").toLong else 0L
            val thumbnailLink: String = if (attribute(11) == null) "empty" else attribute(11).toString
            val commentsDisable: Boolean = if (attribute(12) != null && attribute(12).toString.trim.equalsIgnoreCase("False")) false else true
            val ratingsDisable: Boolean = if (attribute(13) != null && attribute(13).toString.trim.equalsIgnoreCase("False")) false else true
            val videoErrorOrRemoved: Boolean = if (attribute(14) != null && attribute(14).toString.trim.equalsIgnoreCase("False")) false else true
            val description: String = if (attribute(15) == null) "empty" else attribute(15).toString
            val countryCode: String = if (attribute(16) == null) "empty" else attribute(16).toString
            val countryCategoryCode: String = if (attribute(17) == null) "empty" else attribute(17).toString

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

    videoInfoDataRDD

  }

}
