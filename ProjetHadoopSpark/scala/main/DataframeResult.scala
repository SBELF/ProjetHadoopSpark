package main
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import utils.UserDefinedFunction._

object DataframeResult {

  /**
   * Ajoute une colonne 'date' formatée à partir de 'time_ref'.
   */
  def addFormattedDateColumn(df: DataFrame): DataFrame = {
    df.withColumn("date", formatDate(col("time_ref")))
  }

  /**
   * Ajoute une colonne 'year' extraite de la colonne 'date'.
   */
  def addYearColumn(df: DataFrame): DataFrame = {
    df.withColumn("year", year(col("date")))
  }

  /**
   * Associe le code pays à son nom en utilisant une Map et ajoute une colonne 'nom_Pays'.
   */
  def addCountryNameColumn(df: DataFrame, countryMap: Map[String, String]): DataFrame = {
    val countryNameUDF = udf((code: String) => countryMap.getOrElse(code, "Unknown"))
    df.withColumn("nom_Pays", countryNameUDF(col("country_code")))
  }

  /**
   * Ajoute une colonne avec des détails sur les services.
   */
  def addServiceDetailsColumn(df: DataFrame, dfServicesClassification: DataFrame): DataFrame = {
    df.alias("df")
      .join(dfServicesClassification.alias("dfServices"), col("df.code") === col("dfServices.code"), "left")
      .select("df.*", "dfServices.service_label")
      .withColumnRenamed("service_label", "détails_service")
  }

  /**
   * Ajoute une colonne avec des détails sur les goods.
   */
  def addGoodDetailsColumn(df: DataFrame, dfGoodsClassification: DataFrame): DataFrame = {
    df.alias("df")
      .join(dfGoodsClassification.alias("dfGoods"), col("df.code") === col("dfGoods.NZHSC_Level_2_Code_HS4"), "left")
      .select("df.*", "dfGoods.NZHSC_Level_2")
      .withColumnRenamed("NZHSC_Level_2", "détails_good")
  }

  /**
   * Classement des pays exportateurs par goods et par services.
   */
  def rankExportingCountries(df: DataFrame): DataFrame = {
    df.filter(col("account") === "Exports")
      .groupBy("country_code", "product_type")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_exports")
      .orderBy(col("total_exports").desc)
  }
  /**
   * Classement des pays importateurs par goods et par services.
   */
  def rankImportingCountries(df: DataFrame): DataFrame = {
    df.filter(col("account") === "Imports")
      .groupBy("country_code", "product_type")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_imports")
      .orderBy(col("total_imports").desc)
  }

  /**
   * Regroupe les données par good.
   */
  def groupByGood(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Goods")
      .groupBy("code")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_value")
  }

  /**
   * Regroupe les données par service.
   */
  def groupByService(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Services")
      .groupBy("code")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_value")
  }

  /**
   * Liste les services exportés par la France.
   */
  def listFrenchExportedServices(df: DataFrame): DataFrame = {
    df.filter(col("country_code") === "FR" && col("product_type") === "Services")
      .select("code", "value")
  }

  /**
   * Liste les goods importés par la France.
   */
  def listFrenchImportedGoods(df: DataFrame): DataFrame = {
    df.filter(col("country_code") === "FR" && col("product_type") === "Goods")
      .select("code", "value")
  }

  /**
   * Classement des services les moins demandés.
   */
  def rankLeastDemandedServices(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Services")
      .groupBy("code")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_value")
      .orderBy(col("total_value").asc) // Classement par ordre croissant pour les moins demandés
  }
  /**
   * Classement des goods les plus demandés.
   */
  def rankMostDemandedGoods(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Goods")
      .groupBy("code")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_value")
      .orderBy(col("total_value").desc) // Classement par ordre décroissant pour les plus demandés
  }

  /**
   * Ajoute une colonne indiquant le statut import/export pour chaque pays.
   */
  def addImportExportStatus(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("country_code")
    val dfWithTotalImportsExports = df
      .withColumn("total_imports", sum(when(col("account") === "Imports", col("value")).otherwise(0)).over(windowSpec))
      .withColumn("total_exports", sum(when(col("account") === "Exports", col("value")).otherwise(0)).over(windowSpec))

    dfWithTotalImportsExports.withColumn("status_import_export", when(col("total_imports") > col("total_exports"), "négative").otherwise("positive"))
  }

  /**
   * Ajoute une colonne calculant la différence entre les exports et imports pour chaque pays.
   */
  def addDifferenceImportExport(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("country_code")
    df.withColumn("difference_import_export", sum(col("value")).over(windowSpec))
  }

  /**
   * Calcule la somme des goods pour chaque pays.
   */
  def addSumOfGoodsByCountry(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Goods")
      .groupBy("country_code")
      .sum("value")
      .withColumnRenamed("sum(value)", "Somme_good")
  }

  /**
   * Calcule la somme des services pour chaque pays.
   */
  def addSumOfServicesByCountry(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Services")
      .groupBy("country_code")
      .sum("value")
      .withColumnRenamed("sum(value)", "Somme_service")
  }

  /**
   * Calcule le pourcentage de chaque good par rapport au total des goods par pays.
   */
  def addGoodPercentages(df: DataFrame): DataFrame = {
    val totalGoodsByCountry = Window.partitionBy("country_code")
    df.filter(col("product_type") === "Goods")
      .withColumn("Total_goods_country", sum("value").over(totalGoodsByCountry))
      .withColumn("Pourcentage_good", (col("value") / col("Total_goods_country")) * 100)
  }

  /**
   * Calcule le pourcentage de chaque service par rapport au total des services par pays.
   */
  def addServicePercentages(df: DataFrame): DataFrame = {
    val totalServicesByCountry = Window.partitionBy("country_code")
    df.filter(col("product_type") === "Services")
      .withColumn("Total_services_country", sum("value").over(totalServicesByCountry))
      .withColumn("Pourcentage_service", (col("value") / col("Total_services_country")) * 100)
  }

  /**
   * Regroupe les goods selon leur type (Code HS2).
   */
  def groupGoodsByType(df: DataFrame, dfGoodsClassification: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Goods")
      .join(dfGoodsClassification, df("code") === dfGoodsClassification("NZHSC_Level_2_Code_HS4"))
      .groupBy("NZHSC_Level_1_Code_HS2")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_value_by_good_type")
  }

  /**
   * Classement des pays exportateurs de pétrole.
   */
  def rankOilExportingCountries(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Goods" && col("code") === "Code_for_Oil") // Remplacez "Code_for_Oil" par le code réel du pétrole
      .filter(col("account") === "Exports")
      .groupBy("country_code")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_oil_exports")
      .orderBy(col("total_oil_exports").desc)
  }

  /**
   * Classement des pays importateurs de viandes.
   */
  def rankMeatImportingCountries(df: DataFrame): DataFrame = {
    df.filter(col("product_type") === "Goods" && col("code") === "Code_for_Meat") // Remplacez "Code_for_Meat" par le code réel des viandes
      .filter(col("account") === "Imports")
      .groupBy("country_code")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_meat_imports")
      .orderBy(col("total_meat_imports").desc)
  }

  /**
   * Classement des pays selon la demande en services informatiques.
   */
  def rankCountriesByITServiceDemand(df: DataFrame, dfServicesClassification: DataFrame): DataFrame = {
    val itServiceCodes = Seq("Code_for_Computer_Services", "Code_for_Computer_Software", "Code_for_Other_Computer_Services") // Remplacer par les codes réels
    df.filter(col("product_type") === "Services" && col("code").isin(itServiceCodes: _*))                 //presents dans le dataset
      .groupBy("country_code")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_it_services_demand")
      .orderBy(col("total_it_services_demand").desc)
  }

  /**
   * Ajoute une colonne de description indiquant l'activité d'import/export d'un pays sur goods ou services.
   */
  def addDescriptionColumn(df: DataFrame): DataFrame = {
    df.withColumn("description",
      concat_ws(" ",
        lit("Le pays"), col("country_code"),
        lit("fait un"), col("account"),
        lit("sur"), col("product_type"))
    )


  }


}


