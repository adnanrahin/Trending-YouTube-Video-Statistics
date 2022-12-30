package org.trending.youtube.video

import org.apache.spark.sql.functions.{callUDF, col, input_file_name, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{ArrayType, StructType}

object TrendVideoCategoryPreProcessor {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("TrendVideoCategoryPreProcessor")
        .master("local[*]")
        .getOrCreate()

    val jsonFileDF = spark.read.option("multiline", "true")
      .json("data-set/trending_youtube_video_statistics_dataset/category_ids/*.json")

    val FDF = flattenDF(jsonFileDF)

    spark
      .udf
      .register("get_file_name",
        (path: String) => path.split("/")
          .last
          .split("\\.")
          .head
          .substring(0, 2))


    val countryCodeDF = FDF
      .withColumn("country_code",
        callUDF("get_file_name", input_file_name()))

    val countryCategoryCode =
      countryCodeDF.withColumn("country_category_code",
        functions.concat(
          col("country_code"),
          lit("_"),
          col("items_id")
        )
      )

    countryCategoryCode
      .write
      .mode(SaveMode.Overwrite)
      .parquet("data-set/trending_youtube_video_statistics_dataset/video_category")

  }

  private def flattenDF(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    for (i <- fields.indices) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case aType: ArrayType =>
          val firstFieldName = fieldName
          val fieldNamesExcludingArrayType = fieldNames.filter(_ != firstFieldName)
          val explodeFieldNames = fieldNamesExcludingArrayType ++ Array(s"explode_outer($firstFieldName) as $firstFieldName")
          val explodedDf = df.selectExpr(explodeFieldNames: _*)
          return flattenDF(explodedDf)

        case sType: StructType =>
          val childFilenames = sType.fieldNames.map(childName => fieldName + "." + childName)
          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFilenames
          val renamedCols = newFieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
          val explodeDf = df.select(renamedCols: _*)
          return flattenDF(explodeDf)
        case _ =>
      }
    }
    df
  }

}
