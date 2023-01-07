package org.youtube.trending.schemas

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SchemaDefinition {

  final val viewCountSchema = StructType(
    Array(
      StructField("Channel_Title", StringType, true),
      StructField("Total_Views", LongType, true),
    )
  )

}
