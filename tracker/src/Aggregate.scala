import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

case class Aggregate(spark: SparkSession) {

  def buildAggregate(in: String, out: String): Any = {

    def writeData(ds: DataFrame, temp: String) = {
      ds.createOrReplaceTempView("CovidDataByCity")
      val dataByRegion = spark.sql("""
        SELECT date
        , state
        , SUM(cases) as cases
        , SUM(deaths) as deaths 
        FROM CovidDataByCity 
        GROUP BY date, state 
        ORDER BY date, state DESC
        """)
      dataByRegion
        .coalesce(1)
        .write
        .option("header", true)
        .mode("OVERWRITE")
        .csv(temp)
    }

    def renameOutput(temp: String, outPath: String, out: String) = {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val fileName = fs.globStatus(new Path(s"$temp/*.csv"))(0).getPath().getName()
      fs.rename(new Path(temp + "/" + fileName), new Path(s"$outPath/$out"))
    }

    def cleanup(temp: String, outPath: String) = {
      
      import scala.reflect.io.Directory
      import java.io.File
      
      val tempDir = new Directory(new File(temp))
      tempDir.deleteRecursively()
      for {
        files <- Option(new File(outPath).listFiles)
        file <- files if file.getName.endsWith("crc")
      } file.delete()

    }
  
    val outPath = "./data/out/"
    val tempPath = "./data/temp/"
    val data = DataLoaders.LoadCovidData(spark).byCity(in)
    writeData(data, tempPath)
    renameOutput(tempPath, outPath, out)
    cleanup(tempPath, outPath)
  }

}
