package org.trending.youtube.video.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}

object JsonParser {

  def flattenDF(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)

    fields.indices.map { i =>

      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {

        case aType: ArrayType => {

          val firstFieldName = fieldName

          val fieldNamesExcludingArrayType = fieldNames.filter(_ != firstFieldName)

          val explodeFieldNames =
            fieldNamesExcludingArrayType ++ Array(s"explode_outer($firstFieldName) as $firstFieldName")

          val explodedDf = df.selectExpr(explodeFieldNames: _*)

          return flattenDF(explodedDf)

        }

        case sType: StructType => {

          val childFilenames = sType.fieldNames.map(childName => fieldName + "." + childName)

          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFilenames

          val renamedCols =
            newFieldNames
              .map(x =>
                (
                  col(x.toString()).as(x.toString()
                    .replace(".", "_")
                    .replace("$", "_")
                    .replace("__", "_")
                    .replace(" ", "")
                    .replace("-", ""))
                  )
              )

          val explodeDf = df.select(renamedCols: _*)

          return flattenDF(explodeDf)
        }

        case _ =>
          
      }
    }
    df
  }

}
