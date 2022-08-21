import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class Compare(spark: SparkSession) {

  def compare(left: String, right: String) = {
    
    def joiner(ds1: DataFrame, ds2: DataFrame): DataFrame = {

      val ds_left = ds1
          .withColumnRenamed("cases", "cases_left")
          .withColumnRenamed("deaths", "deaths_left")
      
      val ds_right = ds2
          .withColumnRenamed("cases", "cases_right")
          .withColumnRenamed("deaths", "deaths_right")
      
      ds_left.join(ds_right, Seq("date", "state"), "full_outer")
    }

    val data1 = DataLoaders.LoadCovidData(spark).byRegion(left)
    val data2 = DataLoaders.LoadCovidData(spark).byState(right)

    // val leftFileName = ???
    // val rightFileName = ???

    joiner(data1, data2).createOrReplaceTempView("FileCompare")

    val report = spark.sql("""
      SELECT DISTINCT
      (SELECT COUNT(*) AS not_in_right FROM FileCompare WHERE cases_left IS NULL) AS not_in_right
      , (SELECT COUNT(*) AS not_in_left FROM FileCompare WHERE cases_right IS NULL) AS not_in_left
      , (SELECT COUNT(*) AS left_rows FROM FileCompare WHERE cases_left IS NOT NULL) AS rows_left
      , (SELECT COUNT(*) AS right_rows FROM FileCompare WHERE cases_right IS NOT NULL) AS rows_right
      , (SELECT COUNT(*) AS equal_rows 
      FROM FileCompare 
      WHERE 
        cases_right IS NOT NULL 
        AND cases_left IS NOT NULL 
        AND cases_right - cases_left = 0 
        AND deaths_right - deaths_left = 0) AS equal_rows
      , (SELECT COUNT(*) AS unequal_rows 
      FROM FileCompare 
      WHERE
        cases_right IS NOT NULL 
        AND cases_left IS NOT NULL  
        AND (cases_right - cases_left != 0
        OR deaths_right - deaths_left != 0)) AS unequal_rows  
      FROM FileCompare
      """)

    val diff = spark.sql(""" 
      SELECT date, state
      , ABS(cases_right - cases_left) AS cases_diff
      , ABS(deaths_right - deaths_left) AS deaths_diff
      FROM FileCompare 
      ORDER BY date, state DESC
      """)

    diff.createOrReplaceTempView("FileDiff")

    val equalRows = spark.sql("""
      SELECT date, state
      FROM FileDiff
      WHERE cases_diff = 0 AND deaths_diff = 0
      """)

    val unequalRows = spark.sql("""
      SELECT *
      FROM FileDiff 
      WHERE cases_diff != 0 OR deaths_diff != 0
      """)

    val missingRows = spark.sql(""" 
      SELECT date, state
      FROM FileDiff 
      WHERE cases_diff IS NULL
      """)

    // data1.coalesce(1).write.option("header", true).mode("OVERWRITE").csv("./data/out/left")
    // data2.coalesce(1).write.option("header", true).mode("OVERWRITE").csv("./data/out/right")
    // data3.coalesce(1).write.option("header", true).mode("OVERWRITE").csv("./data/out/final")
    // equalRows.coalesce(1).write.option("header", true).mode("OVERWRITE").csv("./data/out/compare/equal")
    // unequalRows.coalesce(1).write.option("header", true).mode("OVERWRITE").csv("./data/out/compare/unequal")
    // missingRows.coalesce(1).write.option("header", true).mode("OVERWRITE").csv("./data/out/compare/missing")
    report.coalesce(1).write.option("header", true).mode("OVERWRITE").csv("./data/out/compare/report")

  }

}