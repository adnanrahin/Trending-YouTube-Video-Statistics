package org.trending.youtube.video.util

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class JsonParserTest extends AnyFunSuite with DataFrameTestUtils {

  test("jsonParserTest") {
    val spark = SparkSession
      .builder()
      .appName("jsonParserTest")
      .master(master = "local[1]")
      .getOrCreate()


    val jsonFileDF = spark.read.option("multiline", "true")
      .json("../data-preprocessing-engine/src/test/resources/JsonTestFile.json")

    val actual = JsonParser.flattenDF(jsonFileDF)

    val data = Seq(
      Row(
        "\"ld9biNPKjAjgjV7EZ4EKeEGrhao/1v2mrzYSYG6onNLt2qTj13hkQZk\"",
        "youtube#videoCategoryListResponse",
        "\"ld9biNPKjAjgjV7EZ4EKeEGrhao/Xy1mB4_yLrHy_BmKmPBggty2mZQ\"",
        "1",
        "youtube#videoCategory",
        true,
        "UCBR8-60-B28hp2BmDPdntcQ",
        "Film & Animation"
      ),
      Row("\"ld9biNPKjAjgjV7EZ4EKeEGrhao/1v2mrzYSYG6onNLt2qTj13hkQZk\"",
        "youtube#videoCategoryListResponse",
        "\"ld9biNPKjAjgjV7EZ4EKeEGrhao/UZ1oLIIz2dxIhO45ZTFR3a3NyTA\"",
        "2",
        "youtube#videoCategory",
        true,
        "UCBR8-60-B28hp2BmDPdntcQ",
        "Autos & Vehicles"
      )
    )

    import spark.sqlContext.implicits._

    val simpleSchema = StructType(Array(
      StructField("etag", StringType, true),
      StructField("kind", StringType, true),
      StructField("items_etag", StringType, true),
      StructField("items_id", StringType, true),
      StructField("items_kind", StringType, true),
      StructField("items_snippet_assignable", BooleanType, true),
      StructField("items_snippet_channelId", StringType, true),
      StructField("items_snippet_title", StringType, true)
    ))

    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(data), simpleSchema
    )

    assert(assertData(actual, expected))

  }

}
