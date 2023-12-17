package utils
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.SaveMode

object Utils {


  /**
   * Crée et renvoie une session Spark.
   */
  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .getOrCreate()
  }

  /**
   * Lit un fichier CSV et renvoie un DataFrame.
   *
   * @param spark La session Spark.
   * @param path Le chemin du fichier CSV à lire.
   * @return DataFrame contenant les données du fichier CSV.
   */
  def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true") // Supposons que la première ligne contient les noms de colonnes
      .option("inferSchema", "true") //pour le data type, Sans cela, Spark traiterait toutes les colonnes comme des chaînes de caractères (String).
      .csv(path)
  }

  /**
   * Écrit un DataFrame dans un fichier CSV.
   *
   * @param df Le DataFrame à écrire.
   * @param path Le chemin où enregistrer le fichier CSV.
   */
  def writeCsv(df: DataFrame, path: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
  }

  /**
   * Écrit un DataFrame dans un fichier Parquet.
   *
   * @param df Le DataFrame à écrire.
   * @param path Le chemin où enregistrer le fichier Parquet.
   */
  def writeParquet(df: DataFrame, path: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }
}

