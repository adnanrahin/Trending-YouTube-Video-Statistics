package org.youtube.trending.schemas

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SchemaDefinition {

  final val viewCountSchema = StructType(
    Array(
      StructField("Channel_Title", StringType, nullable = true),
      StructField("Total_Views", LongType, nullable = true),
    )
  )

  final val mostViewByDateSchema = StructType(
    Array(
      StructField("Dates", StringType, nullable = true),
      StructField("Total_Views", LongType, nullable = true),
    )
  )


}
