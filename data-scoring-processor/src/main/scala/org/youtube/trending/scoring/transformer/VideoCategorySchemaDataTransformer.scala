package org.youtube.trending.scoring.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.youtube.trending.scoring.schemas.VideoCategoryIdSchema

class VideoCategorySchemaDataTransformer(spark: SparkSession, inputPath: String) extends DataTransformer {

  def loadVideoCategoryData(): RDD[VideoCategoryIdSchema] = {

    vidoeCategoryIdTransformer(spark = spark, inputPath = inputPath)

  }

}
