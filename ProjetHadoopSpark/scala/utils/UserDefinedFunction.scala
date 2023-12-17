package utils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.text.SimpleDateFormat
import java.util.Date

object UserDefinedFunction {
  /**
   * UDF pour convertir 'time_ref' (ex: 202206) en format de date (ex: 01/06/2022).
   */
  val formatDate: UserDefinedFunction = udf((timeRef: String) => {
    val inputFormat = new SimpleDateFormat("yyyyMM")
    val outputFormat = new SimpleDateFormat("dd/MM/yyyy")
    val date = inputFormat.parse(timeRef)
    outputFormat.format(date)
  })

  /**
   * UDF pour associer un code pays à son nom complet.
   * @param countryCode Le code du pays.
   * @param countryMap Map des codes pays et de leurs noms complets.
   * @return Le nom complet du pays.
   */
  val countryName: UserDefinedFunction = udf((countryCode: String, countryMap: Map[String, String]) => {
    countryMap.getOrElse(countryCode, "Unknown")
  })

  // On peut rajouter d'autres UDFs selon besoin

}
