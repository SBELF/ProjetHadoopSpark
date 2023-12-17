package main

import utils.Utils._
import main.DataframeResult._
import common.ApplicationProperties._
import common.Constants._
import org.apache.spark.sql.SparkSession

object Main extends App {

  // Créer une session Spark
  val spark: SparkSession = createSparkSession("HadoopSparkProjetPMN")

  // Lire les fichiers CSV en utilisant les chemins définis dans ApplicationProperties et Constants
  val countryClassificationDF = readCsv(spark, inputPath + CountryClassificationFile)
  val outputCsvFullDF = readCsv(spark, inputPath + OutputCsvFullFile)
  val servicesClassificationDF = readCsv(spark, inputPath + ServicesClassificationFile)
  val goodsClassificationDF = readCsv(spark, inputPath + GoodsClassificationFile)

  // Créer une Map pour les noms des pays (à utiliser dans la fonction addCountryNameColumn)
  val countryMap = countryClassificationDF.collect().map(row => row.getString(0) -> row.getString(1)).toMap

  // Appliquer les transformations sur le DataFrame 'outputCsvFullDF'
  val transformedDF = outputCsvFullDF
    .transform(addFormattedDateColumn)
    .transform(addYearColumn)
    .transform(df => addCountryNameColumn(df, countryMap))
    .transform(df => addServiceDetailsColumn(df, servicesClassificationDF))
    .transform(df => addGoodDetailsColumn(df, goodsClassificationDF))
  // +autant de transformations qu'on le souhaite ...

  // Enregistrer le résultat en CSV et Parquet
  writeCsv(transformedDF, outputPath + "result.csv")
  writeParquet(transformedDF, outputPath + "result.parquet")

  // Fermer la session Spark
  spark.stop()
}
