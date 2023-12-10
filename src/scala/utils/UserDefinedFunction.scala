package utils

import org.apache.spark.sql.functions._

object UserDefinedFunction {

  val square: Double => Double = (num: Double) => num * num

  val squareUDF = udf(square)

}
