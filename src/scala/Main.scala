import utils.Utils
object Main extends App {
    val spark = Utils.createSparkSession("YourSparkAppName")

    // Chemin ou sont stocker les CSV
    val countrycsv = "src/main/resources/country_classification.csv"
    val goodscsv = "src/main/resources/goods_classification.csv"
    val outputcsv = "src/main/resources/output_csv_full.csv"
    val servicescsv = "src/main/resources/services_classification.csv"

    // Lire le fichier CSV
    val inputCountryDF = Utils.readCSV(spark, countrycsv)
    val inputGoodsDF = Utils.readCSV(spark, goodscsv)
    val inputOutputDF = Utils.readCSV(spark, outputcsv)
    val inputServiceDF = Utils.readCSV(spark, servicescsv)

    // Afficher les DataFrames
    inputCountryDF.printSchema()
    inputCountryDF.show()
    inputGoodsDF.printSchema()
    inputGoodsDF.show()
    inputOutputDF.printSchema()
    inputOutputDF.show()
    inputServiceDF.printSchema()
    inputServiceDF.show()


    println("Début de l'exécution de addNomPaysColumn")
    val resultDF = DataFrameResult.addNomPaysColumn(
        DataFrameResult.addYearColumn(
            DataFrameResult.addDateColumn(inputOutputDF)
        ),
        inputCountryDF
    )
    println("Fin de l'exécution de addNomPaysColumn")

    println("Début de l'exécution de addDetailsGoodColumn")
    val resultWithDetailsService = DataFrameResult.addDetailsGoodColumn(resultDF, "ServiceFilterValue")
    println("Fin de l'exécution de addDetailsGoodColumn")

    // Afficher le schéma du DataFrame
    resultWithDetailsService.printSchema()

    // Afficher les premières lignes du DataFrame
    resultWithDetailsService.show()

    println("Début de l'exécution de addFormattedDateColumn")
    val resultWithFormattedDate = DataFrameResult.addFormattedDateColumn(resultWithDetailsService)
    println("Fin de l'exécution de addFormattedDateColumn")

    println("Début de l'exécution de addStatusImportExportColumn")
    val resultWithStatus = DataFrameResult.addStatusImportExportColumn(resultWithFormattedDate)
    println("Fin de l'exécution de addStatusImportExportColumn")

    println("Classement des pays exportateurs (par goods et par services):")
    val exportRanking = DataFrameResult.exportRankingByCountry(resultWithStatus)
    exportRanking.show()

    println("Classement des pays importateur :")
    val importRanking = DataFrameResult.importRankingByCountry(resultWithStatus)
    importRanking.show()

    println("Début de l'exécution de addDifferenceImportExportColumn")
    val resultWithDifference = DataFrameResult.addDifferenceImportExportColumn(resultWithStatus)

    println("Fin de l'exécution de addDifferenceImportExportColumn")
    // Afficher le schéma du DataFrame
    resultWithDifference.printSchema()

    // Afficher les premières lignes du DataFrame
    resultWithDifference.show()
    println("Début de l'exécution de addSumGoodsColumn")
    val resultWithSumGoods = DataFrameResult.addSumGoodsColumn(resultWithStatus)
    println("Fin de l'exécution de addSumGoodsColumn")

    // Afficher le schéma du DataFrame
    resultWithSumGoods.printSchema()

    // Afficher les premières lignes du DataFrame
    resultWithSumGoods.show()
    println("Début de l'exécution de addSumServicesColumn")
    val resultWithSumServices = DataFrameResult.addSumServicesColumn(resultWithSumGoods)
    println("Fin de l'exécution de addSumServicesColumn")

    // Afficher le schéma du DataFrame
    resultWithSumServices.printSchema()

    // Afficher les premières lignes du DataFrame
    resultWithSumServices.show()
    println("Début de l'exécution de addPercentageGoodColumn")
    val resultWithPercentageGood = DataFrameResult.addPercentageGoodColumn(resultWithSumServices)
    println("Fin de l'exécution de addPercentageGoodColumn")

    // Afficher le schéma du DataFrame
    resultWithPercentageGood.printSchema()

}