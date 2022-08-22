import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object DataLoaders {

	case class LoadCovidData(spark: SparkSession) {
		
		import spark.implicits._

		def byCity(fileToLoad: String): DataFrame = {
			val data: Dataset[String] = spark.read.text(fileToLoad).as[String]
			val dataParsed = data.flatMap(CovidDataByCity.fromString _)
			val withDate = dataParsed.withColumn("date", to_date(dataParsed("date"), "yyyy-MM-dd"))
			val withCases = withDate.withColumn("cases", dataParsed("cases").cast("float").alias("cases"))
			val withDeaths = withCases.withColumn("deaths", dataParsed("deaths").cast("integer").alias("deaths"))
			val dataByCity = withDeaths.drop("name", "code")
			dataByCity
		}

		def byRegion(fileToLoad: String): DataFrame = {
			val data: Dataset[String] = spark.read.text(fileToLoad).as[String]
			val dataParsed = data.flatMap(CovidDataByRegion.fromString _)
			val withDate = dataParsed.withColumn("date", to_date(dataParsed("date"), "yyyy-MM-dd"))
			val withCases = withDate.withColumn("cases", dataParsed("cases").cast("float").alias("cases"))
			val withDeaths = withCases.withColumn("deaths", dataParsed("deaths").cast("integer").alias("deaths"))
			val dataByRegion = withDeaths.drop("region")
			dataByRegion
		}

		def byState(fileToLoad: String): DataFrame = {
			val data: Dataset[String] = spark.read.text(fileToLoad).as[String]
			val dataParsed = data.flatMap(CovidDataByState.fromString _)
			val withDate = dataParsed.withColumn("date", to_date(dataParsed("date"), "yyyy-MM-dd"))
			val withCases = withDate.withColumn("cases", dataParsed("cases").cast("float").alias("cases"))
			val dataByState = withCases.withColumn("deaths", dataParsed("deaths").cast("integer").alias("deaths"))
			dataByState
		}

	}

}