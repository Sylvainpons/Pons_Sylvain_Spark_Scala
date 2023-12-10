import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object DataFrameResult {
  def addDateColumn(inputDF: DataFrame): DataFrame = {
    inputDF.withColumn("date", date_format(col("time_ref"), "dd/MM/yyyy"))
  }

  def addYearColumn(inputDF: DataFrame): DataFrame = {
    inputDF.withColumn("year", year(to_date(col("date"), "dd/MM/yyyy")))
  }

  def addNomPaysColumn(inputDF: DataFrame, countryDF: DataFrame): DataFrame = {
    inputDF.join(countryDF, inputDF("country_code") === countryDF("country_code"), "left")
      .withColumn("nom_Pays", countryDF("country_label"))
      .drop(countryDF("country_code"))
  }

  def addDetailsGoodColumn(df: DataFrame, goodFilter: String): DataFrame = {
    df.withColumn("details_good", when(col("product_type") === goodFilter, "Good détaillé").otherwise("Autre good"))
  }

  def addFormattedDateColumn(inputDF: DataFrame): DataFrame = {
    inputDF.withColumn("formatted_date", date_format(col("time_ref"), "dd/MM/yyyy"))
  }

  def addStatusImportExportColumn(inputDF: DataFrame): DataFrame = {
    inputDF
      .withColumn("status_import_export",
        when(col("product_type") === "Goods" && col("value") < 0, "négative")
          .when(col("product_type") === "Goods" && col("value") >= 0, "positive")
          .otherwise("Autre")
      )
  }


  def exportRankingByCountry(df: DataFrame): DataFrame = {
    df.groupBy("country_label").agg(sum(when(col("product_type") === "Goods", col("value")).otherwise(0)).as("goods_export"),
        sum(when(col("product_type") === "Services", col("value")).otherwise(0)).as("services_export"))
      .withColumn("total_export", col("goods_export") + col("services_export"))
      .orderBy(desc("total_export"))
  }

  def importRankingByCountry(df: DataFrame): DataFrame = {
    df.groupBy("country_label").agg(sum(when(col("product_type") === "Goods", col("value")).otherwise(0)).as("goods_import"),
        sum(when(col("product_type") === "Services", col("value")).otherwise(0)).as("services_import"))
      .withColumn("total_import", col("goods_import") + col("services_import"))
      .orderBy(desc("total_import"))
  }

  def groupByGood(df: DataFrame): DataFrame = {
    df.groupBy("product_type").agg(sum("value").as("total_value"))
      .orderBy(desc("total_value"))
  }

  def groupByService(df: DataFrame): DataFrame = {
    df.groupBy("service_label").agg(sum("value").as("total_value"))
      .orderBy(desc("total_value"))
  }

  def addDifferenceImportExportColumn(inputDF: DataFrame): DataFrame = {
    inputDF
      .withColumn("difference_import_export",
        when(col("product_type") === "Goods", col("value"))
          .otherwise(0) // Assurez-vous que les lignes de type Service auront une différence de 0
      )
      .groupBy("nom_Pays")
      .agg(sum("difference_import_export").alias("difference_import_export"))
  }
  def addSumGoodsColumn(df: DataFrame): DataFrame = {
    df.groupBy("nom_Pays").agg(sum("value").alias("Somme_good"))
      .join(df, Seq("nom_Pays"), "left")
  }

  def addSumServicesColumn(df: DataFrame): DataFrame = {
    df.groupBy("nom_Pays").agg(sum("value").alias("somme_service"))
      .join(df, Seq("nom_Pays"), "left")
  }

  def addPercentageGoodColumn(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("nom_Pays", "import_export")
    df.withColumn("pourcentage_good", col("value") / sum("value").over(windowSpec))
  }


}
