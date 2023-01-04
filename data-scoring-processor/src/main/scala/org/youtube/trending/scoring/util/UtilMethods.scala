package org.youtube.trending.scoring.util

import scala.util.Try

object UtilMethods {

  final def isNumeric(num: String): Boolean = {

    def isShort(aString: String): Boolean = Try(aString.toLong).isSuccess

    def isInt(aString: String): Boolean = Try(aString.toInt).isSuccess

    def isLong(aString: String): Boolean = Try(aString.toLong).isSuccess

    def isDouble(aString: String): Boolean = Try(aString.toDouble).isSuccess

    def isFloat(aString: String): Boolean = Try(aString.toFloat).isSuccess

    if (isShort(num) || isInt(num) || isLong(num) || isDouble(num) || isFloat(num)) true
    else false
  }

}
