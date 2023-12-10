package utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").csv(path)
  }

  def writeCSV(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write.option("header", "true").csv(path)
  }

  def writeParquet(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write.parquet(path)
  }
}
